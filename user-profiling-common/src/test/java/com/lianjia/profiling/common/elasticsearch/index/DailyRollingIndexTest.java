package com.lianjia.profiling.common.elasticsearch.index;

import com.lianjia.profiling.config.Constants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class DailyRollingIndexTest {

    IndexRoller idx;

    @Before
    public void setUp() throws Exception {
        idx = new IndexRoller("20160501", Constants.ONLINE_USER_IDX, new HashMap<String, String>());
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void test() throws Exception {
        System.out.println(new DateTime(DateTimeZone.forOffsetHours(8)).plusDays(1).withTimeAtStartOfDay().toDate());
        System.out.println(TimeUnit.DAYS.toMillis(1));
        idx.listen();
        // idx.rolling();
        while (true) {
            System.out.println(idx.getIndex());
            Thread.sleep(5000);
        }
    }

}
