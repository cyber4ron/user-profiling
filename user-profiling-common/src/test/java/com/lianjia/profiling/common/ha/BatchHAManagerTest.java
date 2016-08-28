package com.lianjia.profiling.common.ha;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class BatchHAManagerTest {

    @Test
    public void test() throws Exception {
        // test
        // for (int i = 0; i < 5; i++) {
        //     new Thread() {
        //         @Override
        //         public void run() {
        //             try {
        //                 new MasterElection().pollingUntilBecomeMasterUnlessJobFinished();
        //             } catch (Exception e) {
        //                 e.printStackTrace();
        //             }
        //         }
        //     }.start();
        // }

        HAManagerBase x = new BatchHAManager();
        // x.createPersistenceNodeIfAbsent("/profiling/batch/job/20160515");
        // x.deletePath("/profiling/batch/job/20160515");
        x.pollingUntilBecomeMasterUnlessJobFinished();
        // x.markJobAsFinished();

        // mock job
        Thread.sleep(1000 * 1000L);
    }
}
