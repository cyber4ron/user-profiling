package com.lianjia.data.log.appender;

import com.lianjia.data.log.LogQueue;
import org.apache.log4j.Logger;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class ConsoleAppender {
    private static Logger LOG = Logger.getLogger(LocalAppender.class);

    Thread worker;

    public ConsoleAppender(LogQueue<String> queue) {
        final LogQueue<String> q = queue;
        worker = new Thread() {
            @Override
            public void run() {
                try {
                    while (!interrupted()) {
                        System.err.println(q.take());
                    }
                } catch (InterruptedException e) {
                    LOG.warn("worker thread interrupted.");
                    e.printStackTrace();
                }
            }
        };
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        worker.interrupt();
    }
}
