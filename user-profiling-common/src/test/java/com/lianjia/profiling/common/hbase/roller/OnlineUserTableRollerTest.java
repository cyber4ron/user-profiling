package com.lianjia.profiling.common.hbase.roller;

import com.lianjia.profiling.util.DateUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.util.Date;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class OnlineUserTableRollerTest {

    @Test
    public void testRolling() throws Exception {
//        Date x = DateUtil.alignedByMonth(DateUtil.getNextMonth());
//        System.out.println(x);
//        System.out.println(String.format("listening, schedule once, next wake up time: %s...", x));

        System.out.println(new DateTime().withDayOfMonth(1).withTimeAtStartOfDay().getMillis());
        long x = System.currentTimeMillis() - new DateTime().withDayOfMonth(1).withTimeAtStartOfDay().getMillis();
        System.out.println(x);

        System.out.println(DateUtil.alignedByMonth(new Date()));

        System.out.println(x / 24  / 60 / 60 / 1000);
        System.out.println(new Date().toString()); // 本地mac, Tue Jun 21 13:28:07 CST 2016
        System.out.println(new DateTime()); // 本地mac, 2016-06-21T13:28:07.078+08:00

        System.out.println(new LocalDate());

        DateTime dt = new DateTime(DateTimeZone.forOffsetHours(-8));
        System.out.println(dt); // 2016-06-20T21:45:56.341-08:00
        System.out.println(dt.plusMonths(1).toDate());

        dt = new DateTime(DateTimeZone.forOffsetHours(8));
        System.out.println(dt); // 2016-06-21T13:45:56.341+08:00
        System.out.println(dt.plusMonths(1).toDate());

        dt = new DateTime(new Date(), DateTimeZone.forOffsetHours(8)); // 或DateTimeZone.getDefault()
        System.out.println(dt.withDayOfMonth(1).withTimeAtStartOfDay()); // 2016-06-01T00:00:00.000+08:00
        System.out.println(dt.withDayOfMonth(1).withTimeAtStartOfDay().toDate()); // Wed Jun 01 00:00:00 CST 2016

        dt = new DateTime(new Date(), DateTimeZone.forOffsetHours(-8));
        System.out.println(dt.withDayOfMonth(1).withTimeAtStartOfDay()); // 2016-06-01T00:00:00.000-08:00
        System.out.println(dt.withDayOfMonth(1).withTimeAtStartOfDay().toDate()); // Wed Jun 01 16:00:00 CST 2016

        // j.u.Date只包含millisec, 从'epoch': 1970..., 要用DateFormat加timezone.

        // joda的DateTime包含timezone信息, 如果再转到j.u.Date的话, 有可能不一样. 如上例子

        // 如果认为服务器/JVM正确设置了时区. 可以都用默认.
    }
}
