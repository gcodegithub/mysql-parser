package kafka.driver.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.KafkaConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaSender {

    private KafkaConf conf;
    private Producer<String, byte[]> producer;

    public KafkaSender(KafkaConf cf) {
        conf = cf;
    }

    public void connect() {
        Properties prop = new Properties();
        prop.put("metadata.broker.list", conf.brokerList);
        prop.put("serializer.class", conf.serializer);//msg is string
        prop.put("partitioner.class", conf.partitioner);
        prop.put("request.required.acks", conf.acks);
        ProducerConfig pConfig = new ProducerConfig(prop);
        producer = new Producer<String, byte[]>(pConfig);
    }

    public void send(byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
        producer.send(keyMsg);
    }

    public void send(String topic, byte[] msg) {
        KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
        producer.send(keyMsg);
    }

    public void send(List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(conf.topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        producer.send(keyMsgs);
    }

    public void send(String topic, List<byte[]> msgs) {
        List<KeyedMessage<String, byte[]>> keyMsgs = new ArrayList<KeyedMessage<String, byte[]>>();
        for(byte[] msg : msgs) {
            KeyedMessage<String, byte[]> keyMsg = new KeyedMessage<String, byte[]>(topic, null, msg);
            keyMsgs.add(keyMsg);
        }
        producer.send(keyMsgs);
    }

    public void sendKeyMsg(List<KeyedMessage<String, byte[]>> keyMsgs) {
        producer.send(keyMsgs);
    }

    public void close() {
        if(producer != null) producer.close();
    }

}
