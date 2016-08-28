package com.lianjia.profiling.util;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class DateUtilTest {

    @Test
    public void testGetMondayOfCurrentWeek() throws Exception {
        System.out.println(DateUtil.alignedByWeek("20160501"));
        System.out.println(DateUtil.alignedByWeek("20160502"));
        System.out.println(DateUtil.datetimeToTimestampMs("2016-07-07 00:01:57"));
    }
}
