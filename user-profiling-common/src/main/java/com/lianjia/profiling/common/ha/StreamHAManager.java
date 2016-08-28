package com.lianjia.profiling.common.ha;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.DateUtil;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class StreamHAManager extends HAManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(StreamHAManager.class.getName());
    private static final String STREAM_MASTER_NODE = "/profiling/stream/master";
    private static final String STREAM_ALL_PATH = "/profiling/stream/all/";

    public StreamHAManager(String date) throws Exception {
        super(STREAM_MASTER_NODE, "", date);
        String hostName = InetAddress.getLocalHost().getHostName();
        try {
            LOG.info("registering self...");
            createEphemeralNode(STREAM_ALL_PATH + hostName);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }
    }

    public StreamHAManager() throws Exception {
        this(DateUtil.getDate());
    }
}
