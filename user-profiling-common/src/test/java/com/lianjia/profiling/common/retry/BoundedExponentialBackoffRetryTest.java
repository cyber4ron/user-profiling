package com.lianjia.profiling.common.retry;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class BoundedExponentialBackoffRetryTest {
    @Test
    public void testGetRetryTimes() throws Exception {
        BoundedExponentialBackoffRetry retry = new BoundedExponentialBackoffRetry();
        for (int i = 0; i < 100; i++) {
            System.out.println(retry.getSleepTimeMs());
        }
    }
}
