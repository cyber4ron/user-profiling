package com.lianjia.profiling.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

@SuppressWarnings("Duplicates")
public class SimpleTablePool {
    private static Logger logger = LoggerFactory.getLogger(SimpleTablePool.class);
    private static Configuration conf = HBaseConfiguration.create();
    private static Connection connection;

    static {
        try {
            connection = ConnectionFactory.createConnection(conf);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("closing hbase connection...");
                        connection.close();
                        logger.info("hbase connection closed.");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SimpleTablePool() {}

    public static Connection getConn() {
        return connection;
    }

    public static void updateConn() {
        try {
            logger.info("updating conn...");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.warn("update conn failed.", e);
        }
    }

    /**
     * 不支持return, 需要自己close.
     */
    public static Table getTable(String table) {
        Table tbl = null;
        try {
            logger.info("getting table %s...", table);
            tbl = connection.getTable(TableName.valueOf(table));
        } catch (IOException e) {
            logger.warn("get table failed.", e);
        }

        return tbl;
    }
}
