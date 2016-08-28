package com.lianjia.profiling.common.retry;

import com.lianjia.profiling.config.Constants;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class BoundedExponentialBackoffRetry {
    private int baseSleepTimeMs;
    private int maxSleepTimeMs;

    private int retryTimes = 0;

    public BoundedExponentialBackoffRetry() {
        this(Constants.BASE_SLEEP_TIME_MS, Constants.MAX_SLEEP_TIME_MS);
    }

    public BoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepTimeMs = maxSleepTimeMs;
    }

    public long getSleepTimeMs() {
        retryTimes += 1;
        return Math.min(maxSleepTimeMs,
                        (long) baseSleepTimeMs * Math.max(1,(ThreadLocalRandom.current().nextInt(1 << (Math.min(30, retryTimes))))));
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void reset() {
        retryTimes = 0;
    }
}
