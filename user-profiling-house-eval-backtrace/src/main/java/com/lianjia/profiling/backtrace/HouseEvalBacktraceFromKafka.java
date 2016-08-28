package com.lianjia.profiling.backtrace;

import com.lianjia.profiling.common.BlockingBackoffRetryProxy;
import com.lianjia.profiling.stream.LineHandler;
import com.lianjia.profiling.util.DateUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.HashMap;

import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-07
 */

public class HouseEvalBacktraceFromKafka {
    private static final Logger LOG = LoggerFactory.getLogger(HouseEvalBacktraceFromKafka.class.getName());

    private static final String TOPIC = "bigdata-flow-02-log";
    private static final int NUM = 50000;
    private static BlockingBackoffRetryProxy esProxy = new BlockingBackoffRetryProxy(new HashMap<String, String>());

    static class ConsumerLoop extends Thread {
        private final KafkaConsumer<String, String> consumer;

        public ConsumerLoop(String groupId, int num) {

            Properties props = new Properties();
            props.put("bootstrap.servers", com.lianjia.profiling.util.Properties.get("kafka.brokers"));
            props.put("group.id", groupId);
            props.put("auto.offset.reset", "latest");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());

            this.consumer = new KafkaConsumer<>(props);
            assignPartitions(consumer);
            backoff(consumer, num);
        }

        @Override
        public void run() {
            try {
                LOG.info("consumer " + Thread.currentThread().getId() + " is running...");

                DateTime endTime = new DateTime(DateTimeZone.getDefault()).withTimeAtStartOfDay();

                final String NOTICE_TAG = "NOTICE: ";
                final int TAG_LEN = NOTICE_TAG.length();

                int cnt = 0;
                out:
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    for (ConsumerRecord<String, String> record : records) {

                        int pos = record.value().indexOf(NOTICE_TAG);
                        String time = record.value().substring(pos + TAG_LEN, pos + TAG_LEN + 19);

                        DateTime dt = DateUtil.parseDateTime(time);
                        if ((dt != null && dt.compareTo(endTime) > 0) || cnt > NUM) break out;

                        LineHandler.processHouseEval(record.value(), esProxy);
                        cnt++;
                    }
                }

                esProxy.flush();

                LOG.warn("done, cnt = " + cnt);

                LOG.info("consumer shutting down...");
                shutdown();

            } catch (Exception e) {
                LOG.warn("", e);

            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

    @SuppressWarnings("Duplicates")
    public static void backtrace(String consumerGroup, int num) {
        int numConsumers = 1;

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(consumerGroup, num);
            consumers.add(consumer);
            consumer.run();
        }

        for (int i = 0; i < numConsumers; i++) {
            try {
                consumers.get(i).join();
            } catch (InterruptedException e) {
                LOG.warn("", e);
            }
        }
    }

    public static void assignPartitions(KafkaConsumer consumer) {
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition(TOPIC, 0),
                                                        new TopicPartition(TOPIC, 1),
                                                        new TopicPartition(TOPIC, 2));
        consumer.assign(partitions);
    }

    @SuppressWarnings("Duplicates")
    public static void backoff(KafkaConsumer consumer, long offset) {
        TopicPartition partition0 = new TopicPartition(TOPIC, 0);
        TopicPartition partition1 = new TopicPartition(TOPIC, 1);
        TopicPartition partition2 = new TopicPartition(TOPIC, 2);

        LOG.info("partition0 latest offset:" + consumer.position(partition0));
        LOG.info("partition1 latest offset:" + consumer.position(partition1));
        LOG.info("partition2 latest offset:" + consumer.position(partition2));

        long offset0 = Math.max(0, consumer.position(partition0) - offset);
        long offset1 = Math.max(0, consumer.position(partition1) - offset);
        long offset2 = Math.max(0, consumer.position(partition2) - offset);

        LOG.info("partition0 offset:" + offset0);
        LOG.info("partition1 offset:" + offset1);
        LOG.info("partition2 offset:" + offset2);

        consumer.seek(partition0, offset0);
        consumer.seek(partition1, offset1);
        consumer.seek(partition2, offset2);
    }

    public static void main(String[] args) {
        LOG.info("starting house eval backtrace, date: " + new Date().toString());
        int backtraceMessagesNum = NUM;
        if (args.length == 1) {
            backtraceMessagesNum = Integer.parseInt(args[0]);
        }
        LOG.info("num: " + backtraceMessagesNum);
        backtrace(String.valueOf(System.currentTimeMillis()), backtraceMessagesNum);
        LOG.info("exiting house eval backtrace...");
    }
}
