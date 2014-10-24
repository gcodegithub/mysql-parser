package com.jd.bdp.mysql.parser;

import com.jd.bdp.magpie.Topology;
import parser.ParserConfig;

/**
 * Created by hp on 14-9-22.
 */
public class LogParser {

    public static void main(String[] args) throws Exception {
        ParserConfig cnf = new ParserConfig();
        cnf.setHbaseRootDir("hdfs://BJ-YZH-1-H1-3650.jd.com:9000/hbase");
        cnf.setHbaseDistributed("true");
        cnf.setHbaseZkQuorum("BJ-YZH-1-H1-3660.jd.com,BJ-YZH-1-H1-3661.jd.com,BJ-YZH-1-H1-3662.jd.com");
        cnf.setHbaseZkPort("2181");
        cnf.setDfsSocketTimeout("180000");
        HandlerForMagpie handler = new HandlerForMagpie(cnf);
        Topology topology = new Topology(handler);
        topology.run();
    }

}
