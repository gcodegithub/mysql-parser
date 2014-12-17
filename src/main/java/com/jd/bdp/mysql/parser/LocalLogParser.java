package com.jd.bdp.mysql.parser;

import com.jd.bdp.magpie.Topology;
import parser.HandlerForMagpieHBase;
import parser.utils.ParserConfig;

/**
 * Created by hp on 14-11-6.
 */
public class LocalLogParser {

    public static void main(String[] args) throws Exception {
        ParserConfig cnf = new ParserConfig();
        //default config
        cnf.setHbaseRootDir("hdfs://localhost:9000/hbase");
        cnf.setHbaseDistributed("true");
        cnf.setHbaseZkQuorum("127.0.0.1");
        cnf.setHbaseZkPort("2181");
        cnf.setDfsSocketTimeout("180000");
        HandlerForMagpieHBase handler = new HandlerForMagpieHBase(cnf);
        Topology topology = new Topology(handler);
        topology.run();
    }

}
