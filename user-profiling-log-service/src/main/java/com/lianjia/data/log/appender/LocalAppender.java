package com.lianjia.data.log.appender;

import com.lianjia.data.log.Appender;
import com.lianjia.data.log.LogQueue;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * todo: rolling file
 * todo: add http interface
 * todo: flush调整
 * todo: cat打不开?
 * todo: 份应用方道不同文件
 *
 * @author fenglei@lianjia.com on 2016-04
 */
public class LocalAppender implements Appender {
    private static Logger LOG = Logger.getLogger(LocalAppender.class);

    Thread worker;

    public LocalAppender(LogQueue<String> queue, String logPath) {
        final LogQueue<String> q = queue;
        final String p = logPath;
        // todo: check file close?
        worker = new Thread() {
            @Override
            public void run() {
                try {
                    Path path = Paths.get(p);
                    BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);

                    final BufferedWriter w = writer;
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            try {
                                w.close();
                            } catch (IOException e) {
                                LOG.error("closing file error while shutting down.");
                                e.printStackTrace();
                            }
                            System.err.println("====> local appender shut down");
                        }
                    });

                    try {
                        while (!interrupted()) {
                            writer.write(q.take());
                            writer.newLine();
                            writer.flush();
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("worker thread interrupted.");
                        e.printStackTrace();
                    }

                    writer.close();
                } catch (IOException e) {
                    LOG.error("cannot not open file.");
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
