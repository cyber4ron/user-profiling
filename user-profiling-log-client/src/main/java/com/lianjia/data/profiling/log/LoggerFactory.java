package com.lianjia.data.profiling.log;

import com.lianjia.data.profiling.log.client.BlockingLogger;
import com.lianjia.profiling.util.Properties;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class LoggerFactory {
    private static Map<String, Logger> loggerRegistry = new HashMap<>();

    private LoggerFactory() {
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    for (Logger logger : loggerRegistry.values()) {
                        if (logger != null)
                            logger.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static Logger getLogger(Class clazz) {
        return getLogger(clazz.getName(), new HashMap<String, String>());
    }

    public static Logger getLogger(String className) {
        return getLogger(className, new HashMap<String, String>());
    }

    public static Logger getLogger(String className, Map<String, String> conf) {
        if (conf.containsKey("spark.logging.enable") && conf.get("spark.logging.enable").equals("true")
                && conf.containsKey("spark.logging.host") && conf.containsKey("spark.logging.port")) {
            return LoggerFactory.getBlockingLogger(className, conf.get("spark.logging.host"),
                                                   Integer.parseInt(conf.get("spark.logging.port")));
        } else {
            return LoggerFactory.getBlockingLogger(className, Properties.get("logging.host"),
                                                   Integer.parseInt(Properties.get("logging.port")));
        }
    }

    public static synchronized Logger getBlockingLogger(String clazz, String host, int port) {
        if (loggerRegistry.containsKey(clazz)) return loggerRegistry.get(clazz);
        else {
            Logger logger = new BlockingLogger(clazz, host, port);
            loggerRegistry.put(clazz, logger);
            return logger;
        }
    }
}
