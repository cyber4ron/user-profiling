package com.lianjia.profiling.common.redis;

import com.lianjia.profiling.util.ExUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public class PipelinedJedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedJedisClient.class.getName());

    private static final long FIXED_SLEEP_TIME_MS = 30 * 1000;
    private static final int BATCH_SIZE = 10000;
    private static final int MAX_TRY_TIMES = 5;

    private List<Object[]> batch = new ArrayList<>();
    private List<Object[]> kvBatch = new ArrayList<>();

    public synchronized void send(String getKey, String setKey, long inc) {
        batch.add(new Object[]{getKey, setKey, inc});
        if (batch.size() >= BATCH_SIZE) batch();
    }

    private void batch() {
        String curKey = "";
        boolean succ = false;
        int tryTimes = 1;

        while (tryTimes <= MAX_TRY_TIMES && !succ) {
            LOG.info(String.format("batch try %d...", tryTimes));

            try (Jedis jedis = JedisClient.getJedis()) {
                Pipeline p = jedis.pipelined();

                List<Response<String>> getResults = new ArrayList<>(BATCH_SIZE);
                for (Object[] tuple : batch) {
                    String getKey = tuple[0].toString();
                    getResults.add(p.get(getKey));
                }
                p.sync();

                for (int i = 0; i < batch.size(); i++) {
                    String setKey = batch.get(i)[1].toString();
                    long inc = (long) batch.get(i)[2];

                    long org = 0;
                    if (getResults.get(i) != null) {
                        try {
                            org = Long.parseLong(getResults.get(i).get());
                            System.out.println();
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }
                    curKey = setKey;
                    p.set(setKey, String.valueOf(org + inc));

                }
                p.sync();

                succ = true;

            } catch (Exception ex) {
                LOG.warn(String.format("ex in PipelinedJedisClient.batch, key: %s, ex: %s", curKey, ExUtil.getStackTrace(ex)));
                try {
                    Thread.sleep(FIXED_SLEEP_TIME_MS);
                } catch (InterruptedException ignored) {
                }
            } finally {
                tryTimes++;
            }
        }

        batch.clear();
    }

    public synchronized void flush() {
        batch();
    }

    public synchronized void kvSend(String k, String v) {
        kvBatch.add(new Object[]{k, v});
        if (kvBatch.size() >= BATCH_SIZE) kvBatch();
    }

    private void kvBatch() {
        String curKey = "";
        boolean succ = false;
        int tryTimes = 1;

        while (tryTimes <= MAX_TRY_TIMES && !succ) {
            LOG.info(String.format("kvBatch try %d...", tryTimes));

            try (Jedis jedis = JedisClient.getJedis()) {
                Pipeline p = jedis.pipelined();

                for (Object[] tuple : kvBatch) {
                    curKey = tuple[0].toString();
                    p.set(tuple[0].toString(), tuple[1].toString());
                }

                p.sync();

                succ = true;

            } catch (Exception ex) {
                LOG.warn(String.format("ex in PipelinedJedisClient.kvBatch, key: %s, ex: %s", curKey, ExUtil.getStackTrace(ex)));
                try {
                    Thread.sleep(FIXED_SLEEP_TIME_MS);
                } catch (InterruptedException ignored) {
                }
            } finally {
                tryTimes++;
            }
        }

        kvBatch.clear();
    }

    public synchronized void kvFlush() {
        kvBatch();
    }

    public static void main(String[] args) {
        long startMs = System.currentTimeMillis();
        PipelinedJedisClient client = new PipelinedJedisClient();

        // JedisClient.setLongWithExpiration("2000000", 1);
        // for (int i = 2000000; i < 3000000; i++) {
        //     client.send(String.valueOf(Math.max(2000000, i - PipelinedJedisClient.BATCH_SIZE)), String.valueOf(i), 1);
        // }
        // System.out.println(System.currentTimeMillis() - startMs); // 本地, 20678

        for (int i = 3000000; i < 4000000; i++) {
            client.kvSend(String.valueOf(Math.max(2000000, i - PipelinedJedisClient.BATCH_SIZE)), String.valueOf(Math.max(2000000, i - PipelinedJedisClient.BATCH_SIZE)));
        }
        System.out.println(System.currentTimeMillis() - startMs); // 本地, 20678
    }
}
