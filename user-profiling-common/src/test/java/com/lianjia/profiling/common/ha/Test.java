package com.lianjia.profiling.common.ha;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Test {
    public static void main(String[] args) throws Exception {
        CuratorFrameworkFactory.newClient("", null);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("", retryPolicy);
        client.start();
        client.create().forPath("/my/path", null);

        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
        {
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                // this callback will get called when you are the leader
                // do whatever leader work you need to and only exit
                // this method when you want to relinquish leadership
            }
        };

        LeaderSelector selector = new LeaderSelector(client, "", listener);
        selector.autoRequeue(); // not required, but this is behavior that you will probably expect
        selector.start();

        selector.close();
    }
}
