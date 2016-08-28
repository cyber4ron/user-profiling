package com.lianjia.profiling.batch.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class RowParser {
    public static Put parse(String row) {
        return null;
    }

    public static Put[] parseDel(String row) {
        return null;
    }

    public static Put[] parseTouring(String row) {
        return null;
    }

    public static Put[] parseTouringHouse(String row) {
        return null;
    }

    public static Put[] parseHouse(String row) {
        return null;
    }
}

class PutBuilder {
    private Put put;

    private PutBuilder(String rowKey) {
        put = new Put(Bytes.toBytes(rowKey));
    }

    public static PutBuilder newKV(String rowKey) {
        return new PutBuilder(rowKey);
    }

    public PutBuilder addCol(String cf, String cq, long ts, String val) {
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), ts, Bytes.toBytes(val));
        return this;
    }

    public Put get() {
        return put;
    }
}
