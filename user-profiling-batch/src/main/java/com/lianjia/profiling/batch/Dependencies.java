package com.lianjia.profiling.batch;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.DateUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Dependencies {
    private static final Logger LOG = LoggerFactory.getLogger(Dependencies.class.getName(), new HashMap<String, String>());

    public static final String DATABASE = "data_center";
    public static final List<String> TABLES = Arrays.asList("dim_merge_custdel_day",
                                                      "dim_merge_cust_need_day",
                                                      "dim_merge_showing_day",
                                                      "dim_merge_showing_house_day",
                                                      "dim_merge_contract_day",
                                                      "dim_merge_house_day");

    private String date;

    public Dependencies(String date) {
        if (date.length() != 8 || DateUtil.parseDate(date) == null)
            throw new IllegalArgumentException("invalid date format: " + date);
        this.date = date;
    }

    public boolean checkTableExistence() {
        try {
            HiveConf conf = new HiveConf();
            final HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
            for (String table : TABLES) {
                if(!client.tableExists(DATABASE, table)) return false;
            }
            return true;
        } catch (TException e) {
            LOG.warn("", e);
            return false;
        }
    }

    @SuppressWarnings("Duplicates")
    public boolean checkPartitionExistence() {
        try {
            HiveConf conf = new HiveConf();
            final HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
            for (String table : TABLES) {
                client.getPartition(DATABASE, table, "pt=" + date + "000000");
            }
            return true;
        } catch (TException e) {
            LOG.warn("", e);
            return false;
        }
    }

    public void waitUtilReady(long timeMs) {
        while (!checkPartitionExistence()) {
            LOG.info("waiting for hive table exist, sleep %d...", timeMs);
            try {
                Thread.sleep(timeMs);
            } catch (InterruptedException e) {
                LOG.warn("", e);
            }
        }
    }
    public static void main(String[] args) {
        System.out.println(new Dependencies("20160704").checkTableExistence());
    }
}
