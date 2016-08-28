package com.lianjia.profiling.common.hbase.table;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class TableManagerTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCreateOnlineUserTable() throws Exception {

    }

    @Test
    public void testCreateIfNotExists() throws Exception {
        TableManager.createIfNotExists("profiling:online_user_201605");
        TableManager.createIfNotExists("profiling:online_user_idx_201605");
    }
}
