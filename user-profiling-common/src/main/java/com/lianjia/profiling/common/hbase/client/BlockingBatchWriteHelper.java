package com.lianjia.profiling.common.hbase.client;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.common.hbase.table.SimpleTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class BlockingBatchWriteHelper implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingBatchWriteHelper.class.getName(), new HashMap<String, String>());

    private static final int BATCH_SIZE = 3000;
    private static final int RETRY_NUM = 10;

    private List<Row> rows = new ArrayList<>(BATCH_SIZE);

    private Table tbl;

    public BlockingBatchWriteHelper(String table) throws IOException {
        tbl = SimpleTablePool.getTable(table);
        int retries = 0;
        while (tbl == null && retries < RETRY_NUM) {
            try {
                Thread.sleep(5000);
                retries++;
            } catch (InterruptedException e) {
                LOG.warn("", e);
            }
            tbl = SimpleTablePool.getTable(table);
        }

        if (tbl == null) {
            throw new IOException(String.format("can't get hbase table %s after %d tries.", table, retries));
        }
    }

    public void send(Row mut) {
        rows.add(mut);
        if (rows.size() == BATCH_SIZE) flush();
    }

    public void flush() {
        if (rows.size() == 0) return;

        Object[] results = new Object[rows.size()];
        try {
            tbl.batch(rows, results);
        } catch (IOException | InterruptedException e) {
            LOG.warn("", e);
        }

        int succ = 0;
        for (Object result : results) {
            if(result instanceof Result && ((Result) result).value() == null) succ += 1;
        }

        LOG.info("num of put: " + rows.size());
        LOG.info("num of success: " + succ);

        rows.clear();
    }

    @Override
    public void close() throws Exception {
        flush();
        tbl.close();
    }
}
