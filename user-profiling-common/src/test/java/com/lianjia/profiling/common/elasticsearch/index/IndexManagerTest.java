package com.lianjia.profiling.common.elasticsearch.index;

import org.junit.Test;

import java.util.Collections;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class IndexManagerTest {

    @Test
    public void testSetRefreshInterval() throws Exception {
        IndexManager mgr = new IndexManager(Collections.<String, String>emptyMap());
        mgr.setRefreshInterval("customer_20160602", "-1");
    }
}
