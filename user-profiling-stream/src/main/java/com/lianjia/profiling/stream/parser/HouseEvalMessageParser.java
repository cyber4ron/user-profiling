package com.lianjia.profiling.stream.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.stream.MessageUtil;
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder;

import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-05
 */
public class HouseEvalMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(HouseEvalMessageParser.class.getName(), new HashMap<String, String>());

    private static final Set<String> NUMERIC_FIELDS = new HashSet<>(Arrays.asList("cookroom_amount",
                                                                                  "toilet_amount",
                                                                                  "terrace_amount",
                                                                                  "property_fee",
                                                                                  "total_floor",
                                                                                  "balcony_amount",
                                                                                  "parlor_amount",
                                                                                  "build_size",
                                                                                  "distance_metor",
                                                                                  "garden_amount",
                                                                                  "floor",
                                                                                  "bedroom_amount"));
    public static OnlineUserEventBuilder.Doc parse(String line) {
        JSONObject kafkaMessage = JSON.parseObject(line);
        if (!kafkaMessage.containsKey("payload")) return null;

        line = MessageUtil.removeQuotes(kafkaMessage.get("payload").toString());
        String[] parts = line.split("\t");
        if(parts.length < 10) return null;

        JSONObject feat;
        JSONObject input;
        JSONObject resp;

        try {
            String featStr = parts[7];
            String inputStr = parts[8].substring(13);
            String respStr = parts[9].substring(10);

            feat = JSON.parseObject(featStr);
            input = JSON.parseObject(inputStr);
            resp = JSON.parseObject(respStr);
        } catch (Exception e) {
            LOG.warn("failed to parse json of house eval, line: " + line, e);
            return null;
        }

        OnlineUserEventBuilder.Doc doc = new OnlineUserEventBuilder.Doc("house_eval", "house_eval", input.getString("request_id"));
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            if (NUMERIC_FIELDS.contains(entry.getKey())) {
                try {
                    doc.doc.put(entry.getKey(), Float.parseFloat(entry.getValue().toString()));
                } catch (Exception ex) {
                    LOG.warn("cast to float failed.", ex);
                }
            }
            else doc.doc.put(entry.getKey(), entry.getValue());
        }

        if(feat.containsKey("dealdate")) doc.doc.put("dealdate", feat.get("dealdate").toString());
        if(resp.containsKey("rescode")) doc.doc.put("rescode", resp.get("rescode"));
        if(resp.containsKey("resmsg")) doc.doc.put("resmsg", resp.get("resmsg"));
        if(resp.containsKey("details")) doc.doc.put("details", resp.get("details"));

        doc.doc.put("write_ts", System.currentTimeMillis());

        return doc;
    }
}
