package parser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.bdp.magpie.MagpieExecutor;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.driver.consumer.KafkaReceiver;
import kafka.driver.producer.KafkaSender;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.utils.KafkaConf;
import kafka.utils.KafkaMetaMsg;
import monitor.ParserMonitor;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.utils.KafkaPosition;
import parser.utils.ParserConf;
import protocol.avro.EventEntryAvro;
import protocol.protobuf.CanalEntry;
import zk.client.ZkExecutor;
import zk.utils.ZkConf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-12-15.
 */
public class HandlerMagpieKafka implements MagpieExecutor {
    //logger
    private Logger logger = LoggerFactory.getLogger(HandlerMagpieKafka.class);
    //config
    private ParserConf config = new ParserConf();
    //job id
    private String jobId;
    //kafka
    private KafkaSender msgSender;
    //zk
    private ZkExecutor zk;
    //blocking queue
    private BlockingQueue<KafkaMetaMsg> msgQueue;
    //batch id and in batch id
    private long batchId = 0;
    private long inId = 0;
    //thread communicate
    private int globalFetchThread = 0;
    //global var
    private long globalOffset = -1;
    private long globalBatchId = -1;
    private long globalInBatchId = -1;
    //global start time
    private long startTime;
    //thread
    private Fetcher fetcher;
    Timer timer;
    Minuter minter;
    //monitor
    private ParserMonitor monitor;
    //global var
    List<CanalEntry.Entry> entryList;
    CanalEntry.Entry last = null;
    CanalEntry.Entry pre = null;

    //delay time
    private void delay(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void init() throws Exception {
        logger.info("initializing......");
        //load json to global config
//        MagpieConfigJson jcnf = new MagpieConfigJson(jobId);
//        JSONObject jRoot = jcnf.getJson();
//        if(jRoot != null) {
//            JSONObject jcontent = jRoot.getJSONObject("info").getJSONObject("content");
//            config.brokerList = jcontent.getString("brokerList");
//        }
        //test environment
        config.testInit();
        //kafka
        KafkaConf kcnf = new KafkaConf();
        kcnf.brokerList = config.brokerList;
        kcnf.port = config.kafkaPort;
        kcnf.topic = config.topic;
        msgSender = new KafkaSender(kcnf);
        msgSender.connect();
        //zk
        ZkConf zcnf = new ZkConf();
        zcnf.zkServers = config.zkServers;
        zk = new ZkExecutor(zcnf);
        zk.connect();
        initZk();
        //queue
        msgQueue = new LinkedBlockingQueue<KafkaMetaMsg>(config.queuesize);
        //batchid inId
        batchId = 0;
        inId = 0;
        //global thread
        globalFetchThread = 0;
        //global var
        globalOffset = -1;
        globalBatchId = -1;
        globalInBatchId = -1;
        //start time
        startTime = System.currentTimeMillis();
        //thread config
        KafkaConf fetchCnf = new KafkaConf();
        fetchCnf.brokerList = config.brokerList;
        fetchCnf.brokerSeeds = config.brokerSeeds;
        fetchCnf.port = config.kafkaPort;
        fetchCnf.topic = config.topic;
        fetchCnf.partition = config.partition;
        fetcher = new Fetcher(fetchCnf);
        timer = new Timer();
        minter = new Minuter();
        //monitor
        monitor = new ParserMonitor();
        //global var
        entryList = new ArrayList<CanalEntry.Entry>();
    }

    private void initZk() throws Exception {
        if(!zk.exists(config.rootPath)) {
            zk.create(config.rootPath,"");
        }
        if(!zk.exists(config.persisPath)) {
            zk.create(config.persisPath,"");
        }
        if(!zk.exists(config.minutePath)) {
            zk.create(config.minutePath,"");
        }
    }

    public void prepare(String id) throws Exception {
        jobId = id;
        init();
        //start thread
        fetcher.start();
        timer.schedule(minter, 1000, config.minsec * 1000);
        //log
        logger.info("start the parser successfully...");
    }

    class Fetcher extends Thread {
        private Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);
        private KafkaConf conf;
        private List<String> replicaBrokers = new ArrayList<String>();
        public int retry = 3;
        private SimpleConsumer consumer;
        public boolean isFetch = true;
        private ParserMonitor monitor = new ParserMonitor();

        public Fetcher(KafkaConf cnf) {
            conf = cnf;
        }

        public PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partition) {
            PartitionMetadata returnData = null;
            loop:
            for (String broker : brokers) {
                SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, "leader");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse rep = consumer.send(req);
                List<TopicMetadata> topicMetadatas = rep.topicsMetadata();
                for (TopicMetadata topicMetadata : topicMetadatas) {
                    for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                        if(part.partitionId() == partition) {
                            returnData = part;
                            break loop;
                        }
                    }
                }
            }
            if(returnData != null) {
                replicaBrokers.clear();
                for (Broker broker : returnData.replicas()) {
                    replicaBrokers.add(broker.host());
                }
            }
            return returnData;
        }

        public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whitchTime, String clientName) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whitchTime, 1));
            OffsetRequest req = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
            OffsetResponse rep = consumer.getOffsetsBefore(req);
            if(rep.hasError()) {
                logger.error("Error fetching data Offset Data the Broker. Reason: " + rep.errorCode(topic, partition));
                return -1;
            }
            long[] offsets = rep.offsets(topic, partition);
            return offsets[0];
        }

        public String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
            for(int i = 0; i < retry; i++) {
                boolean goToSleep = false;
                PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
                if(metadata == null) {
                    goToSleep = true;
                } else if (metadata.leader() == null) {
                    goToSleep = true;
                } else if(oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                    goToSleep = true;
                } else {
                    return metadata.leader().host();
                }
                if(goToSleep) {
                    delay(1);
                }
            }
            logger.error("Unable to find new leader after Broker failure. Exiting");
            throw new Exception("Unable to find new leader after Broker failure. Exiting");
        }

        private KafkaPosition findPosFromZk() {
            KafkaPosition returnPos = null;
            try {
                String getStr = zk.get(config.persisPath);
                if(getStr == null || getStr.equals("")) {
                    returnPos = new KafkaPosition();
                    returnPos.topic = config.topic;
                    returnPos.partition = config.partition;
                    returnPos.offset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), "get_init");
                    returnPos.batchId = 0;
                    returnPos.inId = 0;
                    return returnPos;
                }
                String[] ss = getStr.split(":");
                if(ss.length !=5) {
                    zk.delete(config.persisPath);
                    logger.error("zk position format is error...");
                    return null;
                }
                returnPos = new KafkaPosition();
                returnPos.topic = ss[0];
                returnPos.partition = Integer.valueOf(ss[1]);
                returnPos.offset = Long.valueOf(ss[2]);
                returnPos.batchId = Long.valueOf(ss[3]);
                returnPos.inId = Long.valueOf(ss[4]);
            } catch (Exception e) {
                logger.error("zk client error : " + e.getMessage());
                e.printStackTrace();
            }
            return returnPos;
        }

        public void run() {
            PartitionMetadata metadata = findLeader(conf.brokerSeeds, conf.port, conf.topic, conf.partition);
            if(metadata == null) {
                logger.error("Can't find metadata for Topic and Partition. Existing");
                return;
            }
            if(metadata.leader() == null) {
                logger.error("Can't find Leader for Topic and Partition. Existing");
                return;
            }
            String leadBroker = metadata.leader().host();
            String clientName = "client_" + conf.topic + conf.partition;
            consumer = new SimpleConsumer(leadBroker, conf.port, 100000, 64 * 1024, clientName);
            //load pos
            KafkaPosition pos = findPosFromZk();
            if(pos == null) {//find position failed......
                globalFetchThread = 1;
                return;
            }
            long readOffset = pos.offset;
            batchId = pos.batchId;
            inId = pos.inId;
            conf.partition = pos.partition;
            conf.topic = pos.topic;
            int numErr = 0;
            while (isFetch) {
                monitor.fetchStart = System.currentTimeMillis();
                if(consumer == null) {
                    consumer = new SimpleConsumer(leadBroker, conf.port, 100000, 64 * 1024, clientName);
                }
                FetchRequest req = new FetchRequestBuilder()
                        .clientId(clientName)
                        .addFetch(conf.topic, conf.partition, readOffset, 100000)
                        .build();
                FetchResponse rep = consumer.fetch(req);
                if(rep.hasError()) {
                    numErr++;
                    short code = rep.errorCode(conf.topic, conf.partition);
                    logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                    if(numErr > 5) {
                        logger.error("5 errors occurred existing the fetching");
                        break;
                    }
                    if(code == ErrorMapping.OffsetOutOfRangeCode()) {
                        readOffset = getLastOffset(consumer, conf.topic, conf.partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                        continue;
                    }
                    consumer.close();
                    consumer = null;
                    try {
                        leadBroker = findNewLeader(leadBroker, conf.topic, conf.partition, conf.port);
                    } catch (Exception e) {
                        logger.error("find lead broker failed");
                        e.printStackTrace();
                        break;
                    }
                    continue;
                }
                numErr = 0;
                long numRead=0;
                for(MessageAndOffset messageAndOffset : rep.messageSet(conf.topic, conf.partition)) {
                    long currentOffset = messageAndOffset.offset();
                    if(currentOffset < readOffset) {
                        logger.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                        continue;
                    }
                    readOffset = messageAndOffset.nextOffset();
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    long offset = readOffset;//now msg + next offset
                    monitor.batchSize += bytes.length;
                    KafkaMetaMsg metaMsg = new KafkaMetaMsg(bytes, offset);
                    try {
                        msgQueue.put(metaMsg);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                        e.printStackTrace();
                    }
                    numRead++;
                }
                if(numRead == 0) {
                    delay(1);//block
                }
                monitor.fetchNum = numRead;
                monitor.fetchEnd = System.currentTimeMillis();
                logger.info("===================================> fetch thread:");
                logger.info("---> fetch thread during sum time : " + (monitor.fetchEnd - monitor.fetchStart));
                logger.info("---> fetch the events (bytes) num : " + monitor.fetchNum);
                logger.info("---> fetch the events size : " + monitor.batchSize);
                monitor.clear();
            }
            consumer.close();
            consumer = null;
        }
    }

    class Minuter extends TimerTask {
        private Logger logger = LoggerFactory.getLogger(Minuter.class);

        public void run() {
            try {
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("HH:mm");
                DateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
                String time = sdf.format(cal.getTime());
                String date = sdfDate.format(cal.getTime());
                String xidValue = config.topic+":"+config.partition+":"+globalOffset+":"+globalBatchId+":"+globalInBatchId;
                if(!zk.exists(config.minutePath+"/"+date)) {
                    zk.create(config.minutePath + "/" + date, date);
                }
                if(!zk.exists(config.minutePath+"/"+date+"/"+time)) {
                    zk.create(config.minutePath + "/" + date + "/" + time, xidValue);
                } else  {
                    zk.set(config.minutePath + "/" + date + "/" + time, xidValue);
                }
                logger.info("===================================> minute thread:");
                logger.info("---> topic is " + config.topic +
                        ",partition is :" + config.partition + ",offset is :" + globalOffset + "; batch id is :" + globalBatchId +
                        ",in batch id is :" + globalInBatchId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isBehind(CanalEntry.Entry pre, CanalEntry.Entry last) {
        if(pre.getBatchId() > last.getBatchId()) {
            return false;
        } else if(pre.getBatchId() < last.getBatchId()) {
            return true;
        } else {
            if(last.getInId() > pre.getInId()) return true;
            else return false;
        }
    }

    public void run() throws Exception {
        //fetch thread's status
        if(globalFetchThread == 1) {
            globalFetchThread = 0;
            reload(jobId);
            return;
        }
        //take the data from the queue
        while (!msgQueue.isEmpty()) {
            KafkaMetaMsg msg = msgQueue.take();
            if(msg == null) continue;
            CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(msg.msg);
            if(pre != null && !isBehind(pre,entry)) continue;//clear the repeat
            pre = entry;
            if(isFilter(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName(), config.disTopic)) {
                CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
                entryBuilder.setHeader(entry.getHeader());
                entryBuilder.setEntryType(entry.getEntryType());
                entryBuilder.setStoreValue(entry.getStoreValue());
                entryBuilder.setBatchId(batchId);
                entryBuilder.setInId(inId);
                CanalEntry.Entry newEntry = entryBuilder.build();
                inId++;
                if(isEnd(entry)) {
                    inId=0;
                    batchId++;
                }
                entryList.add(newEntry);
            }
            globalOffset = msg.offset;
            globalBatchId = batchId;
            globalInBatchId = inId;
            if(entryList.size() >= config.batchsize) break;
        }
        //distribute data to multiple topic kafka, per time must confirm the position
        if(entryList.size() > config.batchsize || (System.currentTimeMillis() - startTime) > config.timeInterval * 1000) {
            monitor.persisNum = entryList.size();
            if(entryList.size() > 0) last = entryList.get(entryList.size()-1);//get the last one,may be enter the time interval and size = 0
            distributeData(entryList);
            confirmPos();
            startTime = System.currentTimeMillis();
            entryList.clear();
        }
        if(monitor.persisNum > 0) {
            logger.info("===================================> persistence thread:");
            logger.info("---> persistence deal during time:" + (monitor.persistenceEnd - monitor.persistenceStart));
            logger.info("---> the number of entry list:" + monitor.persisNum);
            logger.info("---> entry list to bytes (avro) sum size is " + monitor.batchSize);
            if(last != null) logInfoEntry(last);
            logger.info("---> position info:" + " topic is " + config.topic +
                    ",partition is :" + config.partition + ",offset is :" + globalOffset + "; batch id is :" + globalBatchId +
                            ",in batch id is :" + globalInBatchId);
            monitor.clear();
        }
    }

    private void distributeData(List<CanalEntry.Entry> entries) {
        monitor.persistenceStart = System.currentTimeMillis();
        for(CanalEntry.Entry entry : entries) {
            System.out.print("debug :");
            logInfoEntry(entry);
            String topic =  getTopic(entry.getHeader().getSchemaName()+"."+entry.getHeader().getTableName());
            byte[] value = getBytesFromEntryToAvro(entry);
            if(topic == null || value == null) continue;
            msgSender.send(topic, value);
            monitor.batchSize+=value.length;
        }
        monitor.persistenceEnd = System.currentTimeMillis();
    }

    private void confirmPos() throws Exception {
        String value = config.topic+":"+config.partition+":"+globalOffset+":"+batchId+":"+inId;
        zk.set(config.persisPath, value);
    }

    private String getEntryType(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String operationType = "";
            switch (rowChange.getEventType()) {
                case INSERT:
                    return "INSERT";
                case UPDATE:
                    return "UPDATE";
                case DELETE:
                    return "DELETE";
                case CREATE:
                    return "CREATE";
                case ALTER:
                    return "ALTER";
                case ERASE:
                    return "ERASE";
                case QUERY:
                    return "QUERY";
                case TRUNCATE:
                    return "TRUNCATE";
                case RENAME:
                    return "RENAME";
                case CINDEX:
                    return "CINDEX";
                case DINDEX:
                    return "DINDEX";
                default:
                    return "UNKNOWN";
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return "NULL";
    }

    private EventEntryAvro entryToAvro(CanalEntry.Entry entry) {
        EventEntryAvro entryAvro = new EventEntryAvro();
        entryAvro.setDbName(entry.getHeader().getSchemaName());
        entryAvro.setSchema$(entry.getHeader().getSchemaName());
        entryAvro.setTableName(entry.getHeader().getTableName());
        entryAvro.setOperation(getEntryType(entry));
        entryAvro.setDbOptTimestamp(entry.getHeader().getExecuteTime());
        entryAvro.setDmlHBaseOptTimestamp(new Date().getTime());
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            if(rowChange.getIsDdl()) entryAvro.setDdlSql(rowChange.getSql());
            else entryAvro.setDdlSql("");
            entryAvro.setError("");
            //current and source
            for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if(rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    for(CanalEntry.Column column : columns) {
                        sourceCols.put(column.getName(),column.getValue());
                        if(column.getIsKey()) {
                            currentCols.put(column.getName(),column.getValue());
                        }
                    }
                    entryAvro.setCurrent(currentCols);
                    entryAvro.setSource(sourceCols);
                } else if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                    List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    for(CanalEntry.Column column : columns) {
                        currentCols.put(column.getName(),column.getValue());
                    }
                    entryAvro.setSource(sourceCols);
                    entryAvro.setCurrent(currentCols);
                } else if(rowChange.getEventType() == CanalEntry.EventType.UPDATE) {
                    List<CanalEntry.Column> columnsSource = rowData.getBeforeColumnsList();
                    List<CanalEntry.Column> columnsCurrent = rowData.getAfterColumnsList();
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    for(int i=0,j=0;i<=columnsCurrent.size()-1 || j<=columnsSource.size()-1;i++,j++) {
                        if(i<=columnsCurrent.size()-1)
                            currentCols.put(columnsCurrent.get(i).getName(),columnsCurrent.get(i).getValue());
                        if(j<=columnsSource.size()-1)
                            sourceCols.put(columnsSource.get(j).getName(),columnsSource.get(j).getValue());
                    }
                } else {
                    Map<CharSequence, CharSequence> sourceCols = new HashMap<CharSequence, CharSequence>();
                    Map<CharSequence, CharSequence> currentCols = new HashMap<CharSequence, CharSequence>();
                    entryAvro.setCurrent(currentCols);
                    entryAvro.setSource(sourceCols);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return(entryAvro);
    }

    private byte[] getBytesFromAvro(EventEntryAvro avro) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        DatumWriter<EventEntryAvro> writer = new SpecificDatumWriter<EventEntryAvro>(EventEntryAvro.getClassSchema());
        try {
            writer.write(avro,encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] value = out.toByteArray();
        return value;
    }

    private EventEntryAvro getAvroFromBytes(byte[] value) {
        SpecificDatumReader<EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avro;
    }

    private byte[] getBytesFromEntryToAvro(CanalEntry.Entry entry) {
        return getBytesFromAvro(entryToAvro(entry));
    }


    private String getTopic(String macher) {
        Map<String, String> maps = config.disTopic;
        if(maps.size() <= 0) return null;
        Iterator it = maps.keySet().iterator();
        while (it.hasNext()) {
            String regex = it.next().toString();
            if(macher.matches(regex)) return maps.get(regex);
        }
        return null;
    }

    private boolean isEnd(CanalEntry.Entry entry) throws Exception {
        if(entry == null) return false;
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND || (entry.getEntryType() == CanalEntry.EntryType.ROWDATA && rowChange.getIsDdl())) return true;
        else return false;
    }

    private void logInfoEntry(CanalEntry.Entry lastEntry) {
        if(lastEntry != null) {
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(lastEntry.getStoreValue());
                String colValue = "null";
                if (rowChange.getRowDatasList().size() > 0) {
                    CanalEntry.RowData rowData = rowChange.getRowDatas(0);
                    if (rowData.getAfterColumnsList().size() > 0) {
                        colValue = rowData.getAfterColumns(0).getName() + " ## " + rowData.getAfterColumns(0).getValue();
                    }
                }
                logger.info("--->get entry : " +
                                lastEntry.getEntryType() +
                                ", now pos : " +
                                lastEntry.getHeader().getLogfileOffset() +
                                ", next pos : " +
                                (lastEntry.getHeader().getLogfileOffset() + lastEntry.getHeader().getEventLength()) +
                                ", binlog file : " +
                                lastEntry.getHeader().getLogfileName() +
                                ", schema name : " +
                                lastEntry.getHeader().getSchemaName() +
                                ", table name : " +
                                lastEntry.getHeader().getTableName() +
                                ", column info : " +
                                colValue +
                                ", type : " +
                                getEntryType(lastEntry)
                );
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isFilter(String str, Map<String, String> map) {
        if(map.size() == 0) return false;
        Iterator it = map.keySet().iterator();
        while (it.hasNext()) {
            String regex = it.next().toString();
            if(str.matches(regex)) return true;
        }
        return false;
    }

    public void pause(String id) throws Exception {

    }

    public void reload(String id) throws Exception {
        close(jobId);
        prepare(jobId);
    }

    public void close(String id) throws Exception {
        fetcher.isFetch = false;//stop the thread
        minter.cancel();
        timer.cancel();
        zk.close();
    }
}
