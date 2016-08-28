package util;

import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.FieldUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class ESFieldUtilTest {

    @Test
    public void test() throws Exception {
        Assert.assertEquals("", "20150601T203606+0800", DateUtil.toDateTime("2015-06-01 20:36:06"));
        Assert.assertEquals("", null, DateUtil.toDateTime("2015-06-01 20:36"));
        // Assert.assertEquals("", "20150601", DateUtil.toDate("2015-06-01 20:36:06"));
        Assert.assertEquals("", null, DateUtil.toDate("2015-06x-01"));

        Assert.assertEquals("", "20160315", DateUtil.dateDiff("20160316", -1));

        Assert.assertTrue("", FieldUtil.parseInt("10") == 10);
        Assert.assertTrue("", FieldUtil.parseInt("\\N") == null);
        Assert.assertTrue("", FieldUtil.parseInt("n\\a") == null);

        Assert.assertEquals("", null, DateUtil.toDateTime(System.currentTimeMillis()));
    }
}
