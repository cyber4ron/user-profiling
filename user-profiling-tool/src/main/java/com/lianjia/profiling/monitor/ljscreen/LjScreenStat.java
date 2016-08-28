package com.lianjia.profiling.monitor.ljscreen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.profiling.util.RestUtil;
import org.apache.log4j.Logger;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class LjScreenStat {
    private static final Logger LOG = Logger.getLogger(RestUtil.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormat.forPattern("yyyyMMdd");

    public static String addDate(String date, int diff) {
        return DATE_FMT.print(LocalDate.parse(date, DATE_FMT).plusDays(diff));
    }

    private static final Map<String, String> CITY_MAP = new HashMap<>();
    private static final Map<String, String> TYPE_MAP = new HashMap<>();

    static {
        CITY_MAP.put("120000", "天津市");
        CITY_MAP.put("210200", "大连市");
        CITY_MAP.put("310000", "上海市");
        CITY_MAP.put("320100", "南京市");
        // CITY_MAP.put("320500", "苏州市");
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
        CITY_MAP.put("110000", "北京");

        TYPE_MAP.put("customer", "新增客源");
        TYPE_MAP.put("house", "新增房源");
        TYPE_MAP.put("guide", "新增带看");
        TYPE_MAP.put("deal", "新增成交");
    }

    public static String getStatSum() {
        String json = RestUtil.get("10.10.9.16", 8083, "/lianjia_hall_show/get_last_statistics?utime=-1");

        JSONObject obj = JSON.parseObject(json);
        JSONArray arr = ((JSONArray) obj.get("data"));
        StringBuilder htmlBuilder = new StringBuilder();

        htmlBuilder.append("日期: " + ((JSONObject)arr.get(0)).get("start").toString().substring(0, 10) + "<br><br>");
        htmlBuilder.append("全国汇总<br>");

        htmlBuilder.append("<table border=\"1\" >");
        for (Object e : arr) {
            String type = ((JSONObject) e).get("type").toString();
            htmlBuilder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
                                             TYPE_MAP.get(type), ((JSONObject) e).get("stat")));
        }
        htmlBuilder.append("</table>");

        return htmlBuilder.toString();
    }

    public static String toTable(Map<String, Map<String, Integer>> stats) {
        StringBuilder htmlBuilder = new StringBuilder();

        for (Map.Entry<String, Map<String, Integer>> entry : stats.entrySet()) {
            htmlBuilder.append(CITY_MAP.get(entry.getKey()) + "<br>");

            htmlBuilder.append("<table border=\"1\">");

            Map<String, Integer> cityInfo = entry.getValue();
            List<String> keys = Arrays.asList("customer", "house", "guide", "deal");
            for(String key: keys) {
                htmlBuilder.append(String.format("<tr><td>%s</td><td>%d</td></tr>",
                                                 TYPE_MAP.get(key), cityInfo.get(key)));
            }

            htmlBuilder.append("</table><br>");
        }

        return htmlBuilder.toString();
    }

    public static String getStatDetail() {
        Map<String, Map<String, Integer>> stats = new HashMap<>();
        List<String> toGetDate = new ArrayList<>();

        // 从bigdata api取数
        for(String cityId : CITY_MAP.keySet()) {
            stats.put(cityId, getStat(cityId, toGetDate));
        }

        return toTable(stats);
    }

    public static Map<String, Integer> getStat(String city, String date) {
        String previousDate = addDate(date, -1);

        String url = "/api/v1/house-cust-showing/city/day/%city_id?start=%prev&end=%date&access_token=lianjiaweb"
                .replace("%city_id", city)
                .replace("%prev", previousDate)
                .replace("%date", date);
        LOG.debug("url: " + url);

        Map<String, Integer> stat = new HashMap<>();
        JSONObject obj;
        JSONObject data;

        try {
            obj = JSON.parseObject(RestUtil.get(url));
            data = (JSONObject) ((JSONObject) obj.get("data")).get(previousDate);

            try {
                stat.put("customer", Integer.parseInt(data.get("customer_amount").toString()));
            } catch (Exception e) {
                stat.put("customer", -1);
            }

            try {
                stat.put("house", Integer.parseInt(data.get("house_amount").toString()));
            } catch (Exception e) {
                stat.put("house", -1);
            }

            try {
                stat.put("guide", Integer.parseInt(data.get("showing_amount").toString()));
            } catch (Exception e) {
                stat.put("guide", -1);
            }

        } catch (Exception e) {
            stat.put("customer", -1);
            stat.put("house", -1);
            stat.put("guide", -1);
        }

        // 成交
        url = "/api/v1/trans-listed/city/day/%city_id?start=%prev&end=%date&access_token=lianjiaweb"
                .replace("%city_id", city)
                .replace("%prev", previousDate)
                .replace("%date", date);
        LOG.debug("url: " + url);

        try {
            obj = JSON.parseObject(RestUtil.get(url));
            data = (JSONObject) ((JSONObject) obj.get("data")).get(previousDate);

            stat.put("deal", Integer.parseInt(data.get("total_trans_amount").toString()));

        } catch (Exception e) {
            stat.put("deal", -1);
        }

        return stat;
    }

    public static Map<String, Integer> getStat(String city, List<String> dateList) {
        String url = "/api/v1/house-cust-showing/city/day/%city_id?last&access_token=lianjiaweb"
                .replace("%city_id", city);
        LOG.info("url: " + url);

        Map<String, Integer> stat = new HashMap<>();
        JSONObject obj;
        String date;
        JSONObject data;

        try {
            obj = JSON.parseObject(RestUtil.get(url));
            date = (String) ((JSONObject) obj.get("data")).keySet().toArray()[0];
            data = (JSONObject) ((JSONObject) obj.get("data")).get(date);

            dateList.add(date);

            try {
                stat.put("customer", Integer.parseInt(data.get("customer_amount").toString()));
            } catch (Exception e) {
                stat.put("customer", -1);
            }

            try {
                stat.put("house", Integer.parseInt(data.get("house_amount").toString()));
            } catch (Exception e) {
                stat.put("house", -1);
            }

            try {
                stat.put("guide", Integer.parseInt(data.get("showing_amount").toString()));
            } catch (Exception e) {
                stat.put("guide", -1);
            }

        } catch(Exception e) {
            stat.put("customer", -1);
            stat.put("house", -1);
            stat.put("guide", -1);
        }

        // 成交
        url = "/api/v1/trans-listed/city/day/%city_id?last&access_token=lianjiaweb"
                .replace("%city_id", city);
        LOG.info("url: " + url);

        try {
            obj = JSON.parseObject(RestUtil.get(url));
            date = (String) ((JSONObject) obj.get("data")).keySet().toArray()[0];
            data = (JSONObject) ((JSONObject) obj.get("data")).get(date);

            stat.put("deal", Integer.parseInt(data.get("total_trans_amount").toString()));

        } catch (Exception e) {
            stat.put("deal", -1);
        }

        return stat;
    }
}
