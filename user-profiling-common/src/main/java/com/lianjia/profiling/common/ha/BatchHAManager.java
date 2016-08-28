package com.lianjia.profiling.common.ha;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.DateUtil;

import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class BatchHAManager extends HAManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(BatchHAManager.class.getName());
    private static final String BATCH_MASTER_NODE = "/profiling/batch/master";
    private static final String BATCH_FINISHED_TAG_PATH = "/profiling/batch/job/";

    public BatchHAManager(String date) throws UnknownHostException, InterruptedException {
        super(BATCH_MASTER_NODE, BATCH_FINISHED_TAG_PATH, date);
    }

    public BatchHAManager() throws UnknownHostException, InterruptedException {
        super(BATCH_MASTER_NODE, BATCH_FINISHED_TAG_PATH, DateUtil.getDate());
    }
}
