package com.lianjia.profiling.web.util;

import com.lianjia.profiling.web.dao.OlapDao;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class StatUtil {
    public static Map<String, AtomicLong> requests = new HashMap<>();
    public static final long startTimeMs = System.currentTimeMillis();

    public static long getStartupTimeSec() {
        return (System.currentTimeMillis() - startTimeMs) / 1000;
    }

    public static Map<String, Object> getStatistic() {
        Map<String, Object> stat = new HashMap<>();
        stat.put("requests", requests);
        stat.put("up_time", getStartupTimeSec());
        stat.put("sql_dao_queue_remaining", OlapDao.getQueueRemaining());

        return stat;
    }
}
