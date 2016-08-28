package com.lianjia.profiling.tagging.features;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.tagging.tag.HouseTag;
import com.lianjia.profiling.util.DateUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class HouseProperties {

    private Map<HouseTag, Object> entries;

    public HouseProperties() {
        entries = new HashMap<>();
    }

    public void update(HouseTag HouseTag, int idx) {
        entries.put(HouseTag, idx);
    }

    public void update(HouseTag HouseTag, String id) {
        entries.put(HouseTag, id);
    }

    public void update(HouseTag HouseTag, boolean isTrue) {
        entries.put(HouseTag, isTrue? 1 : 0);
    }

    public void updateMeta(HouseTag HouseTag, Object val) {
        entries.put(HouseTag, val);
    }

    public String getId() {
        if(!entries.containsKey(HouseTag.HOUSE_ID)) throw new IllegalStateException("no house id");
        return entries.get(HouseTag.HOUSE_ID).toString();
    }

    public Map<String, Object> toMap() {
        Map<String, Object> doc = new HashMap<>();
        for (Map.Entry<HouseTag, Object> e: entries.entrySet()) {
            doc.put(e.getKey().val(), e.getValue());
        }

        return doc;
    }

    public String toJson() {
        entries.put(HouseTag.WRITE_TS, DateUtil.toDateTime(System.currentTimeMillis()));

        Map<String, Object> doc = new HashMap<>();
        for(Map.Entry<HouseTag, Object> e : entries.entrySet()) {
            doc.put(e.getKey().val(), e.getValue());
        }

        return JSON.toJSONString(doc);
    }
}

