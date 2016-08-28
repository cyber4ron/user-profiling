package com.lianjia.profiling.web.dao;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.common.hbase.client.HBaseClient;
import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.ZipUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class HBaseDao {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseDao.class.getName());
    private static final String ONLINE_USER_TBL_PREFIX = "online_user_";

    private Map<String, HBaseClient> clients = new ConcurrentHashMap<>();

    private String getTable(String date) {
        return ONLINE_USER_TBL_PREFIX + date;
    }

    private HBaseClient getClient(String tableName) throws IOException {
        if (clients.containsKey(tableName)) {
            return clients.get(tableName);
        } else {
            HBaseClient client = new HBaseClient(tableName);
            if (clients.putIfAbsent(tableName, client) == null) return client;
            else {
                client.close();
                return clients.get(tableName);
            }
        }
    }

    public List<Map<String, Object>> getEvents(String table, String key) throws IOException {
        Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> res = getClient(table).get(key.getBytes());
        if (!res.containsKey("evt".getBytes())) return Collections.emptyList();
        return res.get("evt".getBytes()).values().stream().flatMap(x -> x.values().stream())
                .flatMap(val -> {
                    try {
                        return Arrays.asList(ZipUtil.inflate(new String(val)).split("\t")).stream()
                                .map(json -> {
                                    try {
                                        return JSON.parseObject(json).entrySet().stream()
                                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                                          Map.Entry::getValue));
                                    } catch (Exception ex) {
                                        LOG.warn("", ex);
                                        return null;
                                    }
                                }).filter(x -> x != null)
                                .collect(Collectors.toList()).stream();
                    } catch (Exception ex) {
                        return Collections.<Map<String, Object>>emptyList().stream();
                    }

                }).collect(Collectors.toList());
    }

    public List<Map<String, Object>> getEvents(String id, long start, long end) {
        return DateUtil.alignedByMonth(DateUtil.toApiDateTime(start),
                                       DateUtil.toApiDateTime(end)).stream().flatMap(month -> {
            try {
                LOG.info("querying profiling:online_user_event_" + month + ", id: " + id);
                List<Map<String, Object>> x = getEvents("profiling:online_user_event_" + month, id);
                LOG.info("querying returned, profiling:online_user_event_" + month + ", id: " + id);
                return x.stream();
            } catch (IOException ex) {
                LOG.warn("ex in HBaseDao.get", ex);
                return null;
            }

        }).filter(x -> x != null).collect(Collectors.toList());
    }
}
