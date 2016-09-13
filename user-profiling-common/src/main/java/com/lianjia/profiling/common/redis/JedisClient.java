package com.lianjia.profiling.common.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public class JedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(JedisClient.class.getName());

    // todo: pool config
    private static final JedisPool POOL = new JedisPool(new JedisPoolConfig(), "10.10.9.16",
                                                        6379, 3000, "e729647141aae4f859453d8fbabc251b", 10);

    public static final int TEN_DAYS_SEC = 10 * 24 * 60 * 60;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("destroying jedis pool...");
                POOL.destroy();
                LOG.info("jedis pool destroyed.");
            }
        });
    }

    /**
     * 实时累加
     */
    public static long inc(String key, long inc) {
        try (Jedis jedis = POOL.getResource()) {
            return jedis.incrBy(key, inc);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * 每天批处理set计数, key的后缀是date
     */
    public static long setLongWithExpiration(String key, long val) {
        try (Jedis jedis = POOL.getResource()) {
            jedis.set(key, String.valueOf(val));
            return jedis.expire(key, TEN_DAYS_SEC);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * 实时取数
     */
    public static long getLong(String key) {
        try (Jedis jedis = POOL.getResource()) {
            return Long.parseLong(jedis.get(key));
        } catch (Exception e) {
            return -1;
        }
    }

    public static Map<String, Long> multiGetLong(String[] keys) {
        try (Jedis jedis = POOL.getResource()) {
            List<String> values = jedis.mget(keys);
            Map<String, Long> result = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                if (values.get(i).equals("nil")) result.put(keys[i], -1L);
                else {
                    long val = -1;
                    try {
                        val = Long.parseLong(values.get(i));
                    } catch (Exception ignored) {}
                    result.put(keys[i], val);
                }
            }
            return result;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public static String getString(String key) {
        try (Jedis jedis = POOL.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            return null;
        }
    }

    public static Map<String, String> multiGetString(String[] keys) {
        try (Jedis jedis = POOL.getResource()) {
            List<String> values = jedis.mget(keys);
            Map<String, String> result = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                if (values.get(i).equals("nil")) result.put(keys[i], null);
                else {
                    result.put(keys[i], values.get(i));
                }
            }
            return result;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public static long getAndIncAndSetLongWithExpiration(String getKey, String setKey, long inc) {
        try (Jedis jedis = POOL.getResource()) {
            long org = Long.parseLong(jedis.get(getKey));
            jedis.set(setKey, String.valueOf(org + inc));
            return jedis.expire(setKey, TEN_DAYS_SEC);
        } catch (Exception e) {
            return -1;
        }
    }

    public static Jedis getJedis() {
        return POOL.getResource();
    }

    // public static void main(String[] args) {
    //     JedisClient.inc("xx", 1);
    //     JedisClient.getLong("xx");
    //     JedisClient.setLongAndExpire("yy", 999);
    // }
}
