package com.lianjia.profiling.common.ha;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public abstract class HAManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(HAManagerBase.class.getName());
    private String onlineUserMasterNode;
    private String onlineUserFinishedTagPath;
    private CuratorFramework client;
    private Watcher nodeWatcher;
    protected byte[] hostInfo;

    protected HAManagerBase() {
        throw new IllegalStateException();
    }

    protected HAManagerBase(String masterNode, String JobPathPrefix, String date) throws InterruptedException, UnknownHostException {
        onlineUserMasterNode = masterNode;
        onlineUserFinishedTagPath = JobPathPrefix.isEmpty() ? "" : JobPathPrefix + date;
        client = CuratorClient.getClient();
        hostInfo = InetAddress.getLocalHost().toString().getBytes();
        nodeWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                LOG.info("waking up blocking thread...");
                synchronized (HAManagerBase.this) {
                    HAManagerBase.this.notifyAll();
                }
            }
        };
    }

    protected void createEphemeralNode(String path) throws Exception {
        // ephemeral node exists as long as the session is active
        LOG.info("creating " + path + "...");
        client.create().withMode(CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, hostInfo);
    }

    protected void createPersistenceNodeIfAbsent(String path) throws Exception {
        try {
            client.create().withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, hostInfo);
        } catch (KeeperException.NodeExistsException ignored) {
            LOG.info("path [" + path + "] already exists.");
        }
    }

    protected void deletePath(String path) throws Exception {
        client.delete().forPath(path);
    }

    public void pollingUntilBecomeMasterUnlessJobFinished() throws Exception {
        while (true) {
            // check job status
            if (!onlineUserFinishedTagPath.isEmpty() && client.checkExists().forPath(onlineUserFinishedTagPath) != null) {
                LOG.info("job is finished, exiting...");
                System.exit(0);
            }

            // try to be master
            try {
                createEphemeralNode(onlineUserMasterNode);
                LOG.info("became master.");
                return;
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.NODEEXISTS) {
                    throw e;
                }
            }

            // blocking wait
            if (client.checkExists().usingWatcher(nodeWatcher).forPath(onlineUserMasterNode) != null) {
                LOG.info("fail to become master, watched path [" + onlineUserMasterNode + "] and sleeping...");
                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        LOG.info("blocking thread is interrupted, checking job status...");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void markJobAsFinished() {
        while (true) {
            try {
                LOG.info("creating path [" + onlineUserFinishedTagPath + "]...");
                createPersistenceNodeIfAbsent(onlineUserFinishedTagPath);
                Thread.sleep(10000L);
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
