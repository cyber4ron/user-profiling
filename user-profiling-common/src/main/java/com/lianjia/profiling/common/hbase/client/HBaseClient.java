package com.lianjia.profiling.common.hbase.client;

import com.lianjia.profiling.common.hbase.table.SimpleTablePool;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class HBaseClient {

    private Table tbl;

    public HBaseClient(String table) {
        tbl = SimpleTablePool.getTable(table);
    }

    /**
     * todo: tbl是否可用test, 如null等
     */
    public Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> get(byte[] rowKey) throws IOException {
        Get get = new Get(rowKey);

        Result res = tbl.get(get);

        // byte[] row = res.getRow();
        // for(Cell cell : res.listCells()) {
        //     byte[] family = cell.getFamilyArray();
        //     byte[] qualifier = cell.getQualifierArray();
        //     byte[] value = cell.getValueArray();
        // }

        return res.isEmpty() ? Collections.<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>emptyMap()
                : res.getMap();
    }

    public List<NavigableMap<byte[], NavigableMap<byte[],
            NavigableMap<Long, byte[]>>>> filterByPrefix(byte[] prefix) throws IOException {

        Scan scan = new Scan();

        FilterList scanFilter = new FilterList();
        scanFilter.addFilter(new PrefixFilter(prefix));

        scan.setFilter(scanFilter);
        ResultScanner resultScanner = tbl.getScanner(scan);

        List<NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> rows = new ArrayList<>();

        for (Result aResultScanner : resultScanner) {
            rows.add(aResultScanner.getMap());
        }

        return rows;
    }

    public List<NavigableMap<byte[], NavigableMap<byte[],
            NavigableMap<Long, byte[]>>>> filterByRange(byte[] from, byte[] to) throws IOException {

        Scan scan = new Scan();

        FilterList scanFilter = new FilterList();
        scanFilter.addFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(from)));
        scanFilter.addFilter(new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(to)));

        scan.setFilter(scanFilter);
        ResultScanner resultScanner = tbl.getScanner(scan);

        List<NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> rows = new ArrayList<>();

        for (Result aResultScanner : resultScanner) {
            rows.add(aResultScanner.getMap());
        }

        return rows;
    }

    public List<NavigableMap<byte[], NavigableMap<byte[],
            NavigableMap<Long, byte[]>>>> scan(byte[] startRow, byte[] stopRow) throws IOException {

        Scan scan = new Scan(startRow, stopRow);
        ResultScanner resultScanner = tbl.getScanner(scan);

        List<NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> rows = new ArrayList<>();

        for (Result aResultScanner : resultScanner) {
            rows.add(aResultScanner.getMap());
        }

        return rows;
    }

    public void close() throws IOException {
        tbl.close();
    }
}
