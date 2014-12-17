package com.jd.bdp.mysql.parser;

import parser.HandlerMagpieKafka;

/**
 * Created by hp on 14-12-17.
 */
public class LocalParser {

    private static boolean running = true;

    public static void main(String[] args) throws Exception {
        final HandlerMagpieKafka handler = new HandlerMagpieKafka();
        handler.prepare("local-test");
        while (running) {
            handler.run();
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    running = false;
                    handler.close("local-test");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
