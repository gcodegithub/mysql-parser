package com.jd.bdp.mysql.parser;

import com.jd.bdp.magpie.Topology;

/**
 * Created by hp on 14-9-22.
 */
public class LogParser {

    public static void main(String[] args) throws Exception {
        //Handler handler = new Handler();
        Handler1 handler = new Handler1("localhost:9000/hbase");
        Topology topology = new Topology(handler);
        topology.run();
    }

}
