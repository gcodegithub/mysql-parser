package com.jd.bdp.mysql.parser;

import com.jd.bdp.magpie.MagpieExecutor;
import monitor.ParserMonitor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.CanalEntry;
import parser.EntryPrinter;
import parser.HBaseOperator;
import parser.ParserConfig;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 14-9-22.
 */
public class HandlerForMagpie implements MagpieExecutor {

    //parser's logger
    private Logger logger = LoggerFactory.getLogger(HandlerForMagpie.class);

    //configuration
    private ParserConfig configer;

    //hbase operator
    private HBaseOperator hBaseOP;

    //multiple thread queue
    private BlockingQueue<byte[]> bytesQueue;
    private BlockingQueue<byte[]> rowKeyQueue;

    // batch size threshold for per fetch the number of the event,if event size >= batchsize then
    // bigFetch() return
    // now by the py test we set the var is 1000

    private int batchsize = 3000;

    // time threshold if batch size number is not reached then if the time is
    // now by the py test we set the var is 1.5 second
    private double secondsize = 1.5;

    //per seconds write the position
    private int secondPer = 60;

    //Global variables
    private byte[] globalReadPos = null;
    private byte[] globalWritePos = null;

    //control variables
    private boolean running;
    private long startTime;
    private List<byte[]> bytesList;
    private List<byte[]> rowKeyList;

    //monitor
    private ParserMonitor fetchMonitor;
    private ParserMonitor persistenceMonitor;
////    private MonitorToWhaleProducer whaleMonitorProducer;
////    private MonitorToWhaleConsumer whaleMonitorConsumer;
//    private MonitorToKafkaProducer kafkaMonitorProducer;

    //constructor
    public HandlerForMagpie(ParserConfig cnf) {

        this.configer = cnf;
    }

    public HandlerForMagpie(File file) throws IOException{
        if(file.exists()) {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            Properties pro = new Properties();
            pro.load(in);
            configer.setHbaseRootDir(pro.getProperty("hbase.rootdir"));
            configer.setHbaseDistributed(pro.getProperty("hbase.cluster.distributed"));
            configer.setHbaseZkQuorum(pro.getProperty("hbase.zookeeper.quorum"));
            configer.setHbaseZkPort(pro.getProperty("hbase.zookeeper.property.clientPort"));
            configer.setDfsSocketTimeout(pro.getProperty("dfs.socket.timeout"));
        } else {
            logger.error("properties file is not found !!! can not load the task!!!");
            System.exit(1);
        }
    }


    public void prepare(String id) throws Exception {

        //initialize hbase
        hBaseOP = new HBaseOperator(id);
        hBaseOP.getConf().set("hbase.rootdir",configer.getHbaseRootDir());
        hBaseOP.getConf().set("hbase.cluster.distributed",configer.getHbaseDistributed());
        hBaseOP.getConf().set("hbase.zookeeper.quorum",configer.getHbaseZkQuorum());
        hBaseOP.getConf().set("hbase.zookeeper.property.clientPort",configer.getHbaseZkPort());
        hBaseOP.getConf().set("dfs.socket.timeout", configer.getDfsSocketTimeout());
        bytesQueue = new LinkedBlockingQueue<byte[]>();
        rowKeyQueue = new LinkedBlockingQueue<byte[]>();

        //initialize variables
        running = true;
        startTime = new Date().getTime();
        globalWritePos = null;
        globalReadPos =null;
        findStartPos();

        //run parser thread
        //build and start the fetch thread
        FetchThread fetchThread = new FetchThread();
        fetchThread.start();
        //build and start the minute thread
        MinuteTimer minuteThread = new MinuteTimer();
        Timer timer = new Timer();
        timer.schedule(minuteThread, 3 * 1000, secondPer * 1000);

        //persistence variable initialize
        startTime = new Date().getTime();
        bytesList = new ArrayList<byte[]>();
        rowKeyList = new ArrayList<byte[]>();

        //monitor
        fetchMonitor = new ParserMonitor();
        persistenceMonitor = new ParserMonitor();
////        whaleMonitorProducer = new MonitorToWhaleProducer();
////        whaleMonitorProducer.open();
////        whaleMonitorConsumer = new MonitorToWhaleConsumer();
////        whaleMonitorConsumer.open();
//        kafkaMonitorProducer = new MonitorToKafkaProducer();
//        kafkaMonitorProducer.open();

        logger.info("start the mysql-parser successfully...");
    }

    //find the start position to the global read and global write
    private void findStartPos() throws IOException {
        logger.info("find start position for parser...");
        if(!findStartPosHBase()){
            findStartPosDefault();
        }
    }

    //find the start position according to HBase checkpoint table
    private boolean findStartPosHBase() throws IOException{
        Get get = new Get(Bytes.toBytes(hBaseOP.parserRowKey));
        get.addFamily(hBaseOP.getFamily());
        Result result = hBaseOP.getHBaseData(get, hBaseOP.getCheckpointSchemaName());
        byte[] readPos = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol));
        if(readPos != null) {
            String readPosString = Bytes.toString(readPos);
            Long readPosLong = Long.valueOf(readPosString);
            globalReadPos = Bytes.toBytes(readPosLong);
        }
        byte[] writePos = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol));
        if(writePos != null) {
            String writePosString = Bytes.toString(writePos);
            Long writePosLong = Long.valueOf(writePosString);
            globalWritePos = Bytes.toBytes(writePosLong);
        }
        if(globalReadPos == null || globalWritePos == null){
            return(false);
        }else {
            return (true);
        }
    }

    //find the start position by the default value
    private void findStartPosDefault(){
        if(globalReadPos == null) globalReadPos = Bytes.toBytes(0L);
        if(globalWritePos == null) globalWritePos = Bytes.toBytes(0L);
    }

    //fetch thread
    class FetchThread extends Thread {

        //thread logger
        private Logger logger = LoggerFactory.getLogger(FetchThread.class);

        private boolean fetchable = true;

        private int turnCount = 999;//per turn 100 data

        public void run() {
            while(fetchable){
                //while + sleep
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("sleep error!!!");
                    e.printStackTrace();
                }
                //logger.info("fetch data from the HBase...");
                if(isFetchable()) {
                    ResultScanner results = null;
                    Scan scan = new Scan();
                    scan.setBatch(1500);
                    scan.setStartRow(globalReadPos);
                    scan.setStopRow(Bytes.toBytes(Bytes.toLong(globalReadPos) + turnCount));
                    try {
                        results = hBaseOP.getHBaseData(scan, hBaseOP.getEventBytesSchemaName());
                    } catch (IOException e) {
                        logger.error("fetch data failed!!!");
                        e.printStackTrace();
                    }
                    if (results != null) {
                        for (Result result : results) {
                            if (result == null) {//the null is this is the end of batched data
                                break;
                            }
                            byte[] receiveBytes = result.getValue(hBaseOP.getFamily(),
                                    Bytes.toBytes(hBaseOP.eventBytesCol));
                            if (receiveBytes != null) {
                                try {
                                    bytesQueue.put(receiveBytes);
                                } catch (InterruptedException e) {
                                    logger.error("queue put failed!!!");
                                    e.printStackTrace();
                                }
                                globalReadPos = Bytes.toBytes(Bytes.toLong(globalReadPos) + 1L);
                                try {
                                    rowKeyQueue.put(globalReadPos);//the read pos the next pos to read
                                } catch (InterruptedException e) {
                                    logger.error("queue put failed!!!");
                                    e.printStackTrace();
                                }
                                //monitor update
                                fetchMonitor.inEventNum++;
                                fetchMonitor.inSizeBytes += receiveBytes.length;
                                if(fetchMonitor.inEventNum == 1) {
                                    fetchMonitor.startDealTime = new Date().getTime();
                                    fetchMonitor.startTimeDate = new Date();
                                }
                            } else { //the null is this is the end of batched data
                                break;
                            }
                        }
                        //it's a big bug!!!
                        //if we fetched data and persistence the position bug we failed to persistence the data
                        //to hbase entry table then we will lost these data
                        // ,persistence the global read pos
                        /*Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
                        Long readPosLong = Bytes.toLong(globalReadPos);
                        String readPosString = String.valueOf(readPosLong);
                        put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                        try {
                            hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
                        } catch (IOException e) {
                            logger.error("write global read pos failed!!!");
                            e.printStackTrace();
                        }*/
                    }
                }
                //send monitor and after monitor
                fetchMonitor.endDealTime = new Date().getTime();
                fetchMonitor.endTimeDate = new Date();
                fetchMonitor.duringDealTime = fetchMonitor.endDealTime - fetchMonitor.startDealTime;
                if(fetchMonitor.inEventNum > 0) {
                    //whale monitor
//                    try {
////                        whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.inEventNum));
////                        whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.inSizeEvents));
////                        whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.startTimeDate));
////                        whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.endTimeDate));
////                        whaleMonitorProducer.send(0, String.valueOf(fetchMonitor.duringDealTime));
//                        String key = "tracker:" + new Date();
//                        kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.inEventNum));
//                        kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.inSizeEvents));
//                        kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.startTimeDate));
//                        kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.endTimeDate));
//                        kafkaMonitorProducer.send(key, String.valueOf(fetchMonitor.duringDealTime));
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                }
                //after fetchMonitor
                fetchMonitor.clear();
            }
            running = false;//close all running process
        }

        //monitor the hbase globalReadPos whether have inserted data
        private boolean isFetchable(){
            //monitor the hbase globalReadPos whether have the data inserted
            Get get = new Get(globalReadPos);
            get.addColumn(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventBytesCol));
            Result result = null;
            try {
                result = hBaseOP.getHBaseData(get, hBaseOP.getEventBytesSchemaName());
            } catch (IOException e){
                logger.error("fetch single data failed!!!");
                e.printStackTrace();
            }
            if(result == null) return false;
            byte[] receiveBytes = result.getValue(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventBytesCol));
            if(receiveBytes != null) return true;
            else return false;
        }
    }

    //per minute run the function to record the read pos and write pos to checkpoint in HBase
    class MinuteTimer extends TimerTask {

        //logger
        private Logger logger = LoggerFactory.getLogger(MinuteTimer.class);

        public void run() {
            if(globalReadPos != null && globalWritePos != null) {
                //logger.info("per minute persistence the position into HBase...");
                Calendar cal = Calendar.getInstance();
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String time = sdf.format(cal.getTime());
                String rowKey = hBaseOP.parserRowKey + "##" + time;
                Put put = new Put(Bytes.toBytes(rowKey));
                Long readPosLong = Bytes.toLong(globalReadPos);
                String readPosString = String.valueOf(readPosLong);
                Long writePosLong = Bytes.toLong(globalWritePos);
                String writePosString = String.valueOf(writePosLong);
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(writePosString));
                try {
                    hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
                }catch (IOException e){
                    logger.error("minute persistence read pos and write pos failed!!!");
                    e.printStackTrace();
                }
                logger.info("per minute persistence the position into HBase..." +
                            "row key is :" + rowKey + "," +
                            "col is :" + readPosString + "," +
                            "col is :" + writePosString
                );
            }
        }
    }

    public void reload(String id) {
    }


    public void pause(String id) throws Exception {
    }


    public void close(String id) throws Exception {

//        whaleMonitorProducer.close();
//        whaleMonitorConsumer.close();

        //kafkaMonitorProducer.close();

    }


    public void run() throws Exception {
        //logger.info("persistence batched data into HBase 'mysql_entry'...");
        //while + sleep
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error("sleep error!!!");
            e.printStackTrace();
        }
        while(!bytesQueue.isEmpty()) {
            try {
                byte[] receiveBytes = bytesQueue.take();
                bytesList.add(receiveBytes);
                byte[] receiveRowKey = rowKeyQueue.take();
                rowKeyList.add(receiveRowKey);
                //monitor update
                if(receiveBytes != null) {
                    persistenceMonitor.outEventNum++;
                    persistenceMonitor.outSizeEvents += receiveBytes.length;
                    if(persistenceMonitor.outEventNum == 1) {
                        persistenceMonitor.startDealTime = new Date().getTime();
                        persistenceMonitor.startTimeDate = new Date();
                    }
                }
                //per turn do not load much data
                if(bytesList.size() >= batchsize) break;
            } catch (InterruptedException e) {
                logger.error("take data from queue failed!!!");
                e.printStackTrace();
            }
        }
        //persistence the batched size entry string  to entry table in HBase and
        // write pos to checkpoint
        if(bytesList.size() >= batchsize ||
                new Date().getTime() - startTime > secondsize * 1000) {
            if(bytesList.size() > 0) {
                try {
                    //persistence entry data
                    persistenceEntry();
                } catch (IOException e) {
                    logger.error("persistence entry data failed!!!");
                    e.printStackTrace();
                }
                try {
                    //persistence pos data
                    persistencePos();
                } catch (IOException e) {
                    logger.error("persistence write pos failed!!!");
                    e.printStackTrace();
                }
                //clear list
                bytesList.clear();
                rowKeyList.clear();
                startTime = new Date().getTime();
            }
            //monitor update and after monitor
            persistenceMonitor.endDealTime = new Date().getTime();
            persistenceMonitor.endTimeDate = new Date();
            persistenceMonitor.duringDealTime = persistenceMonitor.endDealTime - persistenceMonitor.startDealTime;
            if(persistenceMonitor.outEventNum > 0) {
                //whale monitor
//                try {
////                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.outEventNum));
////                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.outSizeEvents));
////                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.startTimeDate));
////                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.endTimeDate));
////                    whaleMonitorProducer.send(0, String.valueOf(persistenceMonitor.duringDealTime));
//                    String key = "tracker:" + new Date();
//                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.outEventNum));
//                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.outSizeEvents));
//                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.startTimeDate));
//                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.endTimeDate));
//                    kafkaMonitorProducer.send(key, String.valueOf(persistenceMonitor.duringDealTime));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            //after monitor
            persistenceMonitor.clear();
        }
    }

    //persistence entry data
    private void persistenceEntry() throws IOException{
        List<Put> puts = new ArrayList<Put>();
        int i = 0;
        for(byte[] bytes : bytesList) {
            CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(bytes);
            logger.info("--------------------------->get entry : " +
                            entry.getEntryType() +
                            ",-----> now pos : " +
                            entry.getHeader().getLogfileOffset() +
                            ",-----> next pos : " +
                            (entry.getHeader().getLogfileOffset() + entry.getHeader().getEventLength()) +
                            ",-----> binlog file : " +
                            entry.getHeader().getLogfileName() +
                            ",-----> schema name : " +
                            entry.getHeader().getSchemaName() +
                            ",-----> table name : " +
                            entry.getHeader().getTableName()
            );
            String entryString = EntryToString(entry);
            Put put = new Put(globalWritePos);
            put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(entryString));
            puts.add(put);
            globalWritePos = Bytes.toBytes(Bytes.toLong(globalWritePos) + 1L);
            //persistence read pos
            if(i < rowKeyList.size()) {
                Put putKey = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
                Long readPosLong = Bytes.toLong(rowKeyList.get(i));
                String readPosString = String.valueOf(readPosLong);
                putKey.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.eventRowCol), Bytes.toBytes(readPosString));
                try {
                    hBaseOP.putHBaseData(putKey, hBaseOP.getCheckpointSchemaName());
                } catch (IOException e) {
                    logger.error("write global read pos failed!!!");
                    e.printStackTrace();
                }
            }
            i++;
        }
        if(puts.size() > 0) hBaseOP.putHBaseData(puts, hBaseOP.getEntryDataSchemaName());
    }

    //Entry to String
    private String EntryToString(CanalEntry.Entry entry) {
        return(EntryPrinter.printEntry(entry));
    }

    //persistence write pos data
    private void persistencePos() throws IOException {
        if(bytesList.size() > 0) {
            Put put = new Put(Bytes.toBytes(hBaseOP.parserRowKey));
            Long writePosLong = Bytes.toLong(globalWritePos);
            String writePosString = String.valueOf(writePosLong);
            put.add(hBaseOP.getFamily(), Bytes.toBytes(hBaseOP.entryRowCol), Bytes.toBytes(writePosString));
            hBaseOP.putHBaseData(put, hBaseOP.getCheckpointSchemaName());
        }
    }

}
