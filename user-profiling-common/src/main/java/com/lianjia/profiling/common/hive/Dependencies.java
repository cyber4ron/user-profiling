package com.lianjia.profiling.common.hive;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;

import java.util.HashMap;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Dependencies {
    private static final Logger LOG = LoggerFactory.getLogger(Dependencies.class.getName(), new HashMap<String, String>());

    public static boolean checkPartitionExistence(String database, String table, String date) {
        try {
            HiveConf conf = new HiveConf();
            final HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
            client.getPartition(database, table, "pt=" + date + "000000");

            return true;
        } catch (TException e) {
            LOG.warn("", e);
            return false;
        }
    }
}
