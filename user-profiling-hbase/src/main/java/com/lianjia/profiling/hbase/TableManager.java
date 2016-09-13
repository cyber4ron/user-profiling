package com.lianjia.profiling.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

@SuppressWarnings("Duplicates")
public class TableManager {
    private static Logger LOG = LoggerFactory.getLogger(TableManager.class.getName());

    private static final long TBL_MAX_SIZE = 10 * (1L << 30);

    private static final List<String> PREDEFINED_TABLES = new ArrayList<>();

    public static final String ONLINE_USER_EVENT_PREFIX = "profiling:online_user_event_";
    public static final String ONLINE_USER_PREFER_PREFIX = "profiling:online_user_prefer_";


    public static final byte[][] SPLITS = new byte[][]{
            Bytes.toBytes("1"),
            Bytes.toBytes("2"),
            Bytes.toBytes("3"),
            Bytes.toBytes("4"),
            Bytes.toBytes("5"),
            Bytes.toBytes("6"),
            Bytes.toBytes("7"),
            Bytes.toBytes("8"),
            Bytes.toBytes("9"),
            Bytes.toBytes("a"),
            Bytes.toBytes("b"),
            Bytes.toBytes("c"),
            Bytes.toBytes("d"),
            Bytes.toBytes("e"),
            Bytes.toBytes("f")};

    static {
        PREDEFINED_TABLES.add(ONLINE_USER_EVENT_PREFIX);
        PREDEFINED_TABLES.add(ONLINE_USER_PREFER_PREFIX);
    }

    public static synchronized boolean create(String tableName, boolean dropFirst) throws IOException {
        return dropFirst ? dropAndCreate(tableName) : createIfNotExists(tableName);
    }

    public static synchronized boolean dropAndCreate(String tableName) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        try {
            admin.deleteTable(TableName.valueOf(tableName));
            create(tableName);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static synchronized boolean drop(String tableName) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        try {
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static synchronized boolean createIfNotExists(String tableName) throws IOException {
        return exists(tableName) || create(tableName);
    }

    /**
     * 较慢, 测试时1s左右
     */
    public static synchronized boolean exists(String tableName) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.warn("check table existence failed.", e);
            // 有时链接meta表socket超时. 遇到这个情况时只出现在一个executor上.
            // http://jira.lianjia.com/browse/BIGDATADM-9
            // 重新建立连接
            SimpleTablePool.updateConn();

            return false;
        }
    }

    private static synchronized boolean create(String tableName) throws IOException {
        if (tableName.startsWith(ONLINE_USER_EVENT_PREFIX)) {
            return createOnlineUserEventTable(tableName);
        } else if (tableName.startsWith(ONLINE_USER_PREFER_PREFIX)) {
            return createOnlineUserPreferTable(tableName);
        } else
            return false;
    }

    public static synchronized boolean createOnlineUserEventTable(String tableName) throws IOException {
        drop(tableName);

        Admin admin = SimpleTablePool.getConn().getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        try {
            table.setMaxFileSize(TBL_MAX_SIZE);
            table.addFamily(new HColumnDescriptor("evt").setMaxVersions(Integer.MAX_VALUE));

            LOG.info("creating table: " + table.getNameAsString() + "...");
            admin.createTable(table, SPLITS);
            LOG.info("creating table returned, table: " + table.getNameAsString() + ".");

            return true;

        } catch (TableExistsException e) {
            LOG.warn("table " + table.getNameAsString() + " already exists");
            return false;
        }
    }

    public static synchronized boolean createOnlineUserPreferTable(String tableName) throws IOException {
        drop(tableName);

        Admin admin = SimpleTablePool.getConn().getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        try {
            table.setMaxFileSize(TBL_MAX_SIZE);
            table.addFamily(new HColumnDescriptor("prf").setMaxVersions(1));

            LOG.info("creating table: " + table.getNameAsString() + "...");
            admin.createTable(table, SPLITS);
            LOG.info("creating table returned, table: " + table.getNameAsString() + ".");

            return true;

        } catch (TableExistsException e) {
            LOG.warn("table " + table.getNameAsString() + " already exists");
            return false;
        }
    }

    public static void main(String[] args) throws IOException {
        create("profiling:online_user_event_201601", false);
    }
}
