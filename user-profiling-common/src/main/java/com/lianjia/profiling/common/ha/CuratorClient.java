package com.lianjia.profiling.common.ha;

import com.lianjia.profiling.util.Properties;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author fenglei@lianjia.com on 2016-05
 * @see <a href="http://stackoverflow.com/questions/19722354/curator-framework-objects-for-zookeeper"/>
 */
public class CuratorClient {
    private static final String ZK_QUORUM = "zk.quorum";
    private static final int SESSION_TIMEOUT = 60 * 1000;
    private static final int CONNECTION_TIMEOUT = 15 * 1000;
    private static volatile CuratorFramework client;

    private CuratorClient() {
    }

    public static CuratorFramework getClient() throws InterruptedException {
        return getClient(Collections.singletonMap(ZK_QUORUM, Properties.get(ZK_QUORUM)));
    }

    public static CuratorFramework getClient(Map<String, ?> conf) throws InterruptedException {
        if (client == null) {
            synchronized (CuratorClient.class) {
                if (client == null) {
                    RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 10);
                    String zkQuorum = conf.containsKey(ZK_QUORUM) ? conf.get(ZK_QUORUM).toString() : Properties.get(ZK_QUORUM);
                    client = CuratorFrameworkFactory.newClient(zkQuorum,
                                                               SESSION_TIMEOUT,
                                                               CONNECTION_TIMEOUT,
                                                               retryPolicy);
                    client.getConnectionStateListenable().addListener(
                            new ConnectionStateListener() {
                                @Override
                                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                                    if (newState == ConnectionState.LOST) ;
                                }
                            });
                    client.start();
                    client.blockUntilConnected(1, TimeUnit.MINUTES);
                }
            }
        }

        return client;
    }
}
