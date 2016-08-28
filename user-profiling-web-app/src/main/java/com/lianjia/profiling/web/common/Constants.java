package com.lianjia.profiling.web.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Constants {
    public static final int BACKTRACE_DAYS = 180;
    public static final int QUERY_TIMEOUT_MS = 5000;
    public static final int SIZE_LIMIT = 200;
    public static final String SIZE_LIMIT_STR = Integer.toString(SIZE_LIMIT);

    private static final Map<String , String> EVENT_DICT = new HashMap<>();
    private static final Map<String , String> FIELD_DICT = new HashMap<>();

    public enum Event {
        DETAIL("detail", "dtl", "详情页"),
        FOLLOW("follow", "fl", "关注"),
        SEARCH("search", "srh", "搜索"),
        MOB_DETAIL("mobile_detail", "mob_dtl", "移动端详情页"),
        MOB_SEARCH("mobile_search", "mob_srh", "移动端搜索"),
        MOB_FOLLOW("mobile_follow", "mob_fl", "移动端关注"),
        MOB_CLICK("mobile_click", "mob_clk", "移动端点击");

        final String val;
        final String abbr;

        Event(String val, String abbr, String caption) {
            this.val = val;
            this.abbr = abbr;
            EVENT_DICT.put(abbr, caption);
        }

        public String abbr() {
            return abbr;
        }

        public String val() {
            return val;
        }
    }

    static {
        Map<String , String> tmp = new HashMap<>();
        tmp.put("user",              "ucid");
        tmp.put("uuid",                 "uuid");
        tmp.put("city",              "city_id");
        tmp.put("channel",           "ch_id");
        tmp.put("detail_id",            "dtl_id");
        tmp.put("detail_id_type",       "dtl_type");
        tmp.put("keywords",             "kws");
        tmp.put("timestamp",            "ts");
        tmp.put("location",             "loc");
        tmp.put("os_type",              "os");
        tmp.put("event_name",           "evt_name");
        tmp.put("attention_id",         "fl_id");
        tmp.put("attention_action",     "fl_act");
        tmp.put("attention_type",       "fl_type");
        tmp.put("session",           "ssn_id");
        tmp.put("referer_page",         "ref_pg");
        tmp.put("current_page",         "cur_pg");
        tmp.put("out_source_type",      "o_src_type");
        tmp.put("out_source_website",   "o_src_site");
        tmp.put("out_source_link_no",   "o_src_lk_no");
        tmp.put("out_keywords",         "o_kws");
        tmp.put("is_search",            "is_srh");
        tmp.put("search_source",        "srh_src");
        tmp.put("result_number",        "res_num");
        tmp.put("page_index",           "pg_idx");
        tmp.put("search_position",      "srh_pos");
        tmp.put("use_suggestion",       "use_sug");
        tmp.put("sug_position",         "sug_pos");

        tmp.put("district_id",           "ftr_dst_ids");
        tmp.put("price_total_min",        "ftr_prc_min");
        tmp.put("price_total_max",        "ftr_prc_max");
        tmp.put("price_unit_min",      "ftr_u_prc_min");
        tmp.put("price_unit_max",      "ftr_u_prc_max");
        tmp.put("build_type",            "ftr_bld_types");
        tmp.put("house_type",            "ftr_hou_types");
        tmp.put("orientation",          "ftr_ori_ids");
        tmp.put("feature",              "ftr_feats");
        tmp.put("sell_status",           "ftr_sel_status");
        tmp.put("bizcircle_id",          "ftr_ccl_ids");
        tmp.put("subway_line_id",         "ftr_sub_ids");
        tmp.put("bedroom_num",           "ftr_room_num");
        tmp.put("rent_area_min",          "ftr_rt_area_min");
        tmp.put("rent_area_max",          "ftr_rt_area_max");
        tmp.put("house_area_min",         "ftr_hou_area_min");
        tmp.put("house_area_max",         "ftr_hou_area_max");
        tmp.put("floor_level",           "ftr_flr_lv");
        tmp.put("community_id",          "ftr_comm_ids");
        tmp.put("decoration",           "ftr_deco");
        tmp.put("heating_type",          "ftr_heat_types");
        tmp.put("tags",                 "ftr_tags");

        tmp.put("channel",              "ftr_ch");
        tmp.put("city",              "ftr_city_id");
        tmp.put("district_id",          "ftr_dist_id");
        tmp.put("max_price",            "ftr_max_prc");
        tmp.put("min_price",            "ftr_min_prc");
        tmp.put("max_area",             "ftr_max_area");
        tmp.put("min_area",             "ftr_min_area");
        tmp.put("room_count",           "ftr_room_num");
        tmp.put("subway_line_id",       "ftr_sub_id");
        tmp.put("subway_station_id",    "ftr_sub_st_id");
        tmp.put("queryStr",             "ftr_qry");
        tmp.put("is_suggestion",        "ftr_is_sug");

        for (Map.Entry<String, String> entry : tmp.entrySet()) {
            FIELD_DICT.put(entry.getValue(), entry.getKey());
        }
    }

    public enum DetailType {
        HOUSE("房源"),
        BROKER("经纪人"),
        BLOCK("小区");

        String val;

        public String val() {
            return val;
        }

        public static  String get(int i) {
            switch (i) {
                case 1:
                    return HOUSE.val();
                case 2:
                    return BROKER.val();
                case 3:
                    return BLOCK.val();
                default:
                    return "unknown";
            }
        }

        DetailType(String caption) {
            this.val = caption;
        }
    }

    public static String getEventVal(String key) {
        return EVENT_DICT.getOrDefault(key, key);
    }

    public static String getFieldValue(String abbr) {
        if(FIELD_DICT.containsKey(abbr)) return FIELD_DICT.get(abbr);
        else return abbr;
    }

    public static final String CUST_IDX = "customer";
    public static final String CUST_TYPE = "customer";
    public static final String CUST_DEL_IDX = "customer_delegation";
    public static final String CUST_TOUR_IDX = "customer_touring";
    public static final String CUST_CONTRACT_IDX = "customer_contract";

    public static final String HOUSE_IDX = "house";
    public static final String HOUSE_TYPE = "house";

    public static final String ONLINE_USER_IDX_PREFIX = "online_user_";
    public static final List<Event> ONLINE_EVENTS = Arrays.asList(Event.DETAIL,
                                                                  Event.FOLLOW,
                                                                  Event.SEARCH,
                                                                  Event.MOB_DETAIL,
                                                                  Event.MOB_SEARCH,
                                                                  Event.MOB_FOLLOW,
                                                                  Event.MOB_CLICK);

    public static final String UCID_MAP_IDX = "ucid_phone";

    public static final String HOUSE_PROP_IDX = "house_prop";
    public static final String HOUSE_PROP_TYPE = "prop";
}
