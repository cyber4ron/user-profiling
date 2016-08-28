package com.lianjia.profiling.common.hbase.builder;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class PutBuilder {
    private Put put;
    private String rowKey;
    private String cf;
    private String cq;
    private long ts;

    private PutBuilder() {}

    public static PutBuilder newPut() {
        return new PutBuilder();
    }

    public void setIdentity(String rowKey, String cf, String cq) {
        this.cf = cf;
        this.cq = cq;
        this.rowKey = rowKey;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public PutBuilder addCol(String cf, String cq, long ts, String val) {
        put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), ts, Bytes.toBytes(val));
        return this;
    }

    public PutBuilder addCol(String val) {
        put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), ts, Bytes.toBytes(val));
        return this;
    }

    public Put get() {
        return put;
    }
}
