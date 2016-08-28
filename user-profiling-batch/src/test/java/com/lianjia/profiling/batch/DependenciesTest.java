package com.lianjia.profiling.batch;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class DependenciesTest {
    @Test
    public void testWaitUtilReady() throws Exception {
        Dependencies dependencies = new Dependencies("20160502");
        System.out.println(dependencies.checkPartitionExistence());
        dependencies.waitUtilReady(1000);
    }
}
