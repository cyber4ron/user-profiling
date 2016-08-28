package test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ConsumerLoop implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    private final List<String> tempMessages = new ArrayList<>();

    public ConsumerLoop(int id,
                        String groupId,
                        List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaTest.SERVERS);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        // KafkaTest.assignPartitions(consumer);
        // KafkaTest.rewindOffset(consumer);
    }

    @Override
    public void run() {
        try {
            int cnt = 0;
            List<String> messages = new ArrayList<>();

            out: while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
//                    if (record.value().contains("[bigdata.")) {
//                        System.out.println(this.id + ": " + data);
//                    }
                    System.out.println(this.id + ": " + data);
                    tempMessages.add(this.id + ": " + data);
                    // if(data.get("value").toString().contains("2016-06-30")) {
                    //     messages.add(data.get("value").toString());
                    //     cnt++;
                    // }

                    // if(data.get("value").toString().contains("2016-07-01")) break out;
                }
            }

            // try {
            //     Files.write(Paths.get("message_house_eval"), messages, Charset.defaultCharset());
            // } catch (IOException e) {
            //     e.printStackTrace();
            // }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}

public class KafkaTest {

    public static final String SERVERS = "jx-op-kafka03.zeus.lianjia.com:9092,jx-op-kafka04.zeus.lianjia.com:9092,jx-op-kafka05.zeus.lianjia.com:9092,jx-op-kafka06.zeus.lianjia.com:9092";
    @SuppressWarnings("Duplicates")
    public static void test(Set<String> topicSet, String consumerGroup) {
        int numConsumers = 3;
        String groupId = consumerGroup;
        List<String> topics = new ArrayList<>();
        topics.addAll(topicSet);

        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void assignPartitions(KafkaConsumer consumer) {
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("bigdata-flow-02-log", 0),
                                                        new TopicPartition("bigdata-flow-02-log", 1),
                                                        new TopicPartition("bigdata-flow-02-log", 2));
        consumer.assign(partitions);
    }

    /**
     * https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
     */
    public static void rewindOffset(KafkaConsumer consumer) {
        TopicPartition partition = new TopicPartition("bigdata-flow-02-log", 0);
        TopicPartition partition2 = new TopicPartition("bigdata-flow-02-log", 1);
        TopicPartition partition3 = new TopicPartition("bigdata-flow-02-log", 2);

        // consumer.seekToBeginning(partition);
        consumer.seek(partition, 45000);
        consumer.seek(partition2, 45000);
        consumer.seek(partition3, 45000);
    }

    public static void backoff(KafkaConsumer consumer, long offset) {
        TopicPartition partition = new TopicPartition("bigdata-flow-02-log", 0);
        TopicPartition partition2 = new TopicPartition("bigdata-flow-02-log", 1);
        TopicPartition partition3 = new TopicPartition("bigdata-flow-02-log", 2);

        consumer.seek(partition, consumer.position(partition) - offset);
        consumer.seek(partition2, consumer.position(partition2) - offset);
        consumer.seek(partition3, consumer.position(partition3) - offset);
    }

    // 10.10.6.39:9092,10.10.6.41:9092,10.10.5.72:9092 lianjiaApp test-pf-0001
    public static void main(String[] args) {
        String initServers;
        String topicToFilter;
        String consumerGroup;

        if (args.length >= 3) {
            initServers = args[0];
            topicToFilter = args[1];
            consumerGroup = args[2];
        } else {
            initServers = "jx-op-kafka03.zeus.lianjia.com:9092,jx-op-kafka04.zeus.lianjia.com:9092,jx-op-kafka05.zeus.lianjia.com:9092,jx-op-kafka06.zeus.lianjia.com:9092";
            // initServers = "jx-lj-kafka01.vm.lianjia.com:9092,jx-lj-kafka02.vm.lianjia.com:9092,jx-lj-kafka03.vm.lianjia.com:9092";
            topicToFilter = "bigdata-flow-02-log";
            consumerGroup = "x000011";
        }

        System.out.println(initServers);
        System.out.println(topicToFilter);
        System.out.println(consumerGroup);

        Properties props = new Properties();
        props.put("bootstrap.servers", initServers);
        props.put("group.id", consumerGroup);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        System.out.println("consumer created.");

        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        for (String key : topics.keySet()) {
            System.out.println(String.format("%s: %s", key, topics.get(key).size()));
        }

        System.out.println("testing...");
        // test(topics.keySet());
        test(new HashSet<>(Arrays.asList(topicToFilter)), consumerGroup);

        System.out.println("returning...");
    }
}
