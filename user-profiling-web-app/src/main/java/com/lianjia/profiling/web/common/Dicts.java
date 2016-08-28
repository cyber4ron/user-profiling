package com.lianjia.profiling.web.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Dicts {
    private static Map<Integer, String> channels = new HashMap<>();
    private static Map<Integer, String> cities = new HashMap<>();
    private static Map<Integer, String> sources = new HashMap<>();
    private static Map<Integer, String> pageSources = new HashMap<>();
    private static Map<Integer, String> searchSources = new HashMap<>();
    private static Map<Integer, String> detailTypes = new HashMap<>();
    private static Map<Integer, String> followTypes = new HashMap<>();

    private static Map<Integer, String> mobilePageSources = new HashMap<>();
    private static Map<Integer, String> mobileSearchSources = new HashMap<>();

    static {
        channels.put(1, "首页");
        channels.put(2, "二手房");
        channels.put(3, "新房");
        channels.put(4, "租房");
        channels.put(5, "商铺");
        channels.put(6, "小区");
        channels.put(7, "业主");
        channels.put(8, "经纪人");
        channels.put(9, "问答");
        channels.put(10, "设置、登陆");
        channels.put(11, "地铁房(二手房)");
        channels.put(12, "学区房(二手房)");
        channels.put(13, "成交记录(二手房)");
        channels.put(14, "地图找房");
        channels.put(15, "数据频道");
        channels.put(16, "业主卖房");

        cities.put(120000, "天津");
        cities.put(210200, "大连");
        cities.put(310000, "上海");
        cities.put(320100, "南京");
        cities.put(320500, "苏州");
        cities.put(330100, "杭州");
        cities.put(350200, "厦门");
        cities.put(370101, "济南");
        cities.put(370200, "青岛");
        cities.put(420100, "武汉");
        cities.put(430100, "长沙");
        cities.put(440100, "广州");
        cities.put(440300, "深圳");
        cities.put(440600, "佛山");
        cities.put(500000, "重庆");
        cities.put(510100, "成都");
        cities.put(110000, "北京");

        sources.put(1, "直接访问");
        sources.put(2, "付费搜索");
        sources.put(3, "免费搜索");
        sources.put(4, "站内");
        sources.put(5, "站外");

        pageSources.put(1, "首页");
        pageSources.put(2, "二手房列表");
        pageSources.put(3, "地铁房列表");
        pageSources.put(4, "学区房列表");
        pageSources.put(5, "成交记录列表");
        pageSources.put(6, "新房列表");
        pageSources.put(7, "租赁列表");
        pageSources.put(8, "经纪人列表");
        pageSources.put(20, "二手房详情页");
        pageSources.put(21, "新房详情页");
        pageSources.put(22, "租赁详情页");
        pageSources.put(23, "经纪人详情页");

        searchSources.put(1, "搜索栏");
        searchSources.put(2, "地图");
        searchSources.put(3, "搜索栏翻页");

        followTypes.put(1, "租房");
        followTypes.put(2, "二手房");
        followTypes.put(3, "小区");
        followTypes.put(4, "学校");
        followTypes.put(5, "新房");
        followTypes.put(6, "搜索条件");

        detailTypes.put(1, "房源");
        detailTypes.put(2, "经纪人");
        detailTypes.put(3, "小区");
        detailTypes.put(4, "学校");

        mobileSearchSources.put(1, "搜索栏");
        mobileSearchSources.put(2, "地图");
        mobileSearchSources.put(3, "搜索栏翻页");

        mobilePageSources.put(1, "二手房详情页");
        mobilePageSources.put(2, "二手房列表页");
        mobilePageSources.put(3, "新房详情页");
        mobilePageSources.put(4, "新房列表页");
        mobilePageSources.put(5, "租房详情页");
        mobilePageSources.put(6, "租房列表页");
        mobilePageSources.put(7, "小区详情页");
        mobilePageSources.put(8, "小区二手房列表页");
        mobilePageSources.put(9, "小区新房列表页");
        mobilePageSources.put(10, "小区租房列表页");
    }

    private static <K> String get(Map<K, String> dict, K id) {
        return dict.containsKey(id) ? dict.get(id) : "其他(" + id + ")";
    }

    public static String getChannel(int id) {
        return get(channels, id);
    }

    public static String getCity(int id) {
        return get(cities, id);
    }

    public static String getSources(int id) {
        return get(sources, id);
    }

    public static String getPageSource(int id) {
        return get(pageSources, id);
    }

    public static String getSearchSource(int id) {
        return get(searchSources, id);
    }

    public static String getDetailType(int id) {
        return get(detailTypes, id);
    }

    public static String getFollowType(int id) {
        return get(followTypes, id);
    }

    public static String getMobilePageSource(int id) {
        return get(mobilePageSources, id);
    }

    public static String getMobileSearchSource(int id) {
        return get(mobileSearchSources, id);
    }

    public static final Map<String, Function<Integer, String>> ID_MAP = new HashMap<>();
    static {
        ID_MAP.put("city_id", Dicts::getCity);
        ID_MAP.put("ftr_city_id", Dicts::getCity);
        ID_MAP.put("ftr_ch", Dicts::getChannel);
        ID_MAP.put("ch_id", Dicts::getChannel);
        ID_MAP.put("o_src_type", Dicts::getSources);
        ID_MAP.put("ref_pg", Dicts::getPageSource);
        ID_MAP.put("ref_pg", Dicts::getPageSource);
        ID_MAP.put("cur_pg", Dicts::getSearchSource);
        ID_MAP.put("srh_src", Dicts::getSearchSource);
        ID_MAP.put("fl_type", Dicts::getFollowType);
        ID_MAP.put("dtl_type", Dicts::getDetailType);
    }
}
