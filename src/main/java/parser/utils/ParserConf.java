package parser.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 14-12-15.
 */
public class ParserConf {
    //kafka conf
    public static String brokerList = "localhost:9092";//"12:9092,13.9092,14:9092"
    public static int kafkaPort = 9092;
    public static String zk = "localhost:2181";
    public static String serializer = "kafka.serializer.DefaultEncoder";//default is byte[]
    public static String partitioner = "kafka.producer.DefaultPartitioner";
    public static String acks = "1";
    public static String topic = "test";//queue topic
    public static int partition = 0;
    public static List<String> topics = new ArrayList<String>();//distribute the multiple topic
    public static List<String> brokerSeeds = new ArrayList<String>();//"12,13,14"
    public static Map<String, String> disTopic = new HashMap<String, String>();
    //zk conf
    public String zkServers = "127.0.0.1:2181";//"48:2181,19:2181,50:2181"
    public int timeout = 100000;
    public String rootPath = "/mysql_parser";
    public String persisPath = rootPath + "/persis";
    public String minutePath = rootPath + "/minutes";
    //parser conf <database.table, topic> database.table could be a regex
    public int batchsize = 10000;
    public int minsec = 60;
    public int timeInterval = 1;

    static  {
        brokerSeeds.add("127.0.0.1");
        disTopic.put("canal_test\\.simple", "aa");
        disTopic.put("canal_test\\.test", "bb");
        disTopic.put("canal_test\\.filter", "aa");
    }
}
