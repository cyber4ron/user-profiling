package com.lianjia.profiling.common.hbase.table;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class TableManager {
    private static Logger LOG = LoggerFactory.getLogger(TableManager.class.getName());

    private static final long TBL_MAX_SIZE = 10 * (1L << 30);

    private static final List<String> PREDEFINED_TABLES = new ArrayList<>();

    static {
        PREDEFINED_TABLES.add("profiling:online_user");
        PREDEFINED_TABLES.add("profiling:online_user_idx");
    }

    public static boolean create(String tableName, boolean dropFirst) throws IOException {
        return dropFirst ? dropAndCreate(tableName) : createIfNotExists(tableName);
    }

    public static boolean dropAndCreate(String tableName) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        try {
            admin.deleteTable(TableName.valueOf(tableName));
            create(tableName);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    public static boolean createIfNotExists(String tableName) throws IOException {
        return exists(tableName) || create(tableName);
    }

    /**
     * 较慢, 测试时1s左右
     */
    public static boolean exists(String tableName) throws IOException {
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

    private static boolean create(String tableName) throws IOException {
        boolean isPredefined = false;
        for (String tablePrefix : PREDEFINED_TABLES) {
            if (tableName.startsWith(tablePrefix)) {
                isPredefined = true;

                if (tablePrefix.equals("profiling:online_user")) {
                    createOnlineUserTable(tableName);
                } else if (tablePrefix.equals("profiling:online_user_idx")) {
                    createOnlineUserTable(tableName);
                }

                break;
            }
        }

        if (!isPredefined) {
            LOG.warn("unknown table: %s", tableName);
        }

        return isPredefined;
    }

    private static boolean createOnlineUserTable(String tableName) throws IOException {
        return createOnlineUserTable(tableName, new String[0]);
    }

    public static void majorCompact(String tableName) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        admin.majorCompact(TableName.valueOf(tableName));
    }

    private static boolean createOnlineUserTable(String tableName, String[] splitHint) throws IOException {
        Admin admin = SimpleTablePool.getConn().getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        byte[][] splits = new byte[][]{
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

        try {
            table.setMaxFileSize(TBL_MAX_SIZE);
            table.addFamily(new HColumnDescriptor("web")
                                    //.setCompressionType(Compression.Algorithm.SNAPPY) // snappy集群未装
                                    //.setCompressionType(Compression.Algorithm.GZ)
                                    .setMaxVersions(Integer.MAX_VALUE));

            table.addFamily(new HColumnDescriptor("mob")
                                    //.setCompressionType(Compression.Algorithm.SNAPPY) // snappy集群未装
                                    //.setCompressionType(Compression.Algorithm.GZ)
                                    .setMaxVersions(Integer.MAX_VALUE));

            // clear and create
            if (admin.tableExists(TableName.valueOf(table.getNameAsString()))) {
                admin.disableTable(TableName.valueOf(table.getNameAsString()));
                admin.deleteTable(TableName.valueOf(table.getNameAsString()));
            }
            LOG.info("creating table: " + table.getNameAsString() + "...");
            admin.createTable(table, splits);
            LOG.info("creating table returned, table: " + table.getNameAsString() + ".");

            return true;

        } catch (TableExistsException e) {
            LOG.warn("table " + table.getNameAsString() + " already exists");
            return false;
        }
    }

    public static void main(String[] args) {
        try {
            TableManager.createOnlineUserTable("profiling");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
