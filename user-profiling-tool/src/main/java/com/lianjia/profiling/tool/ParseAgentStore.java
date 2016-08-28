package com.lianjia.profiling.tool;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-08
 */

public class ParseAgentStore {
    private static final Map<String, String> CITY_MAP = new HashMap<>();

    static {
        CITY_MAP.put("120000", "天津市");
        CITY_MAP.put("210200", "大连市");
        CITY_MAP.put("310000", "上海市");
        CITY_MAP.put("320100", "南京市");
        CITY_MAP.put("320500", "苏州市");
        CITY_MAP.put("330100", "杭州市");
        CITY_MAP.put("350200", "厦门市");
        CITY_MAP.put("370101", "济南市");
        CITY_MAP.put("370200", "青岛市");
        CITY_MAP.put("420100", "武汉市");
        CITY_MAP.put("430100", "长沙市");
        CITY_MAP.put("440100", "广州市");
        CITY_MAP.put("440300", "深圳市");
        CITY_MAP.put("440600", "佛山市");
        CITY_MAP.put("500000", "重庆市");
        CITY_MAP.put("510100", "成都市");
        CITY_MAP.put("110000", "北京市");
    }

    public static void main(String[] args) throws IOException {
        Map<String, Map<String, List<String>>> map = new HashMap<>();
        Map<String, String> areaMap = new HashMap<>();
        Map<String, String> storeMap = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("user-profiling-tool/src/main/resources/agents"))) {
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split("\\t");
                String city = parts[0];
                String area = parts[1];
                String store = parts[3];

                areaMap.put(parts[1], parts[2]);
                storeMap.put(parts[3], parts[4]);

                if (!map.containsKey(city)) map.put(city, new HashMap<String, List<String>>());
                if (!map.get(city).containsKey(area)) map.get(city).put(area, new ArrayList<String>());
                map.get(city).get(area).add(store);

                line = reader.readLine();
            }
        }

        System.out.println(JSON.toJSONString(CITY_MAP));
        // System.out.println();
        // System.out.println(JSON.toJSONString(areaMap));
        // System.out.println(JSON.toJSONString(storeMap));
    }
}
