package com.lianjia.profiling.config;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class Constants {
    public static final int BASE_SLEEP_TIME_MS = 1000;
    public static final int MAX_SLEEP_TIME_MS = 30 * 1000;

    public static final int PROXY_BUFFER_QUEUE_SIZE = 1000;
    public static final int ES_BULK_REQUEST_SIZE_THD = 1000;

    public static final String ONLINE_USER_IDX = "online_user";
    public static final String ONLINE_USER_TBL = "profiling:online_user";

    public static final double COUNTING_PRECISION = 0.001;
}
