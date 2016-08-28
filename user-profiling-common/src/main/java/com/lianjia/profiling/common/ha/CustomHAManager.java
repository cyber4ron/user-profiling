package com.lianjia.profiling.common.ha;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.DateUtil;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class CustomHAManager extends HAManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CustomHAManager.class.getName());

    public CustomHAManager(String masterNode, String finishedTagPath, String date) throws Exception {
        super(masterNode, finishedTagPath, date);
    }

    public CustomHAManager(String masterNode, String finishedTagPath) throws Exception {
        this(masterNode, finishedTagPath, DateUtil.getDate());
    }
}
