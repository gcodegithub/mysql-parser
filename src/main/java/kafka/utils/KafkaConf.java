package kafka.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 14-12-12.
 */
public class KafkaConf {

    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int port = 9092;
    public static String zk = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String acks = "1";
    public static String topic;//queue topic
    public static int partition = 0;
    public static List<String> topics = new ArrayList<String>();//distribute the multiple topic
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"

}
