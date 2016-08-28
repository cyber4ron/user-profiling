package com.lianjia.profiling;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class KafkaHelper {
    public static void createTopic() {
        // zookeeper servers, http://monitor-dashboard.lianjia.com:18081
        String zookeeperConnect = "jx-lj-zookeeper01.vm.lianjia.com:2181,jx-lj-zookeeper02.vm.lianjia.com:2181,jx-lj-zookeeper03.vm.lianjia.com:2181";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;
        //ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

        String topic = "test8899";
        int partitions = 2;
        int replication = 3;
        Properties topicConfig = new Properties(); // add per-topic configurations settings here
        //AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
        zkClient.close();
    }

    public static void main(String[] args) {
        createTopic();
    }
}
