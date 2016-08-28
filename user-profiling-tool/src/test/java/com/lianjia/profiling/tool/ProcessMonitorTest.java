package com.lianjia.profiling.tool;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class ProcessMonitorTest {
    @Test
    public void test() {
        System.out.println(new DateTime(DateTimeZone.forOffsetHours(8)).plusDays(1).withTimeAtStartOfDay().toDate());
    }
}
