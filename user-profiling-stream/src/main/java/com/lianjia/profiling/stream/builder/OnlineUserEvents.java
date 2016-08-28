package com.lianjia.profiling.stream.builder;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class OnlineUserEvents {
    /**
     * append only
     */
    public enum Event {
        USER                ("00",    "usr"),
        MOBILE_USER         ("01",    "mob_usr"),
        DETAIL              ("02",   "dtl"),
        SEARCH              ("03",   "srh"),
        FOLLOW              ("04",   "fl"),
        MOBILE_DETAIL       ("05",   "mob_dtl"),
        MOBILE_SEARCH       ("06",   "mob_srh"),
        MOBILE_FOLLOW       ("07",   "mob_fl"),
        MOBILE_OTHER_CLICK  ("08",   "mob_clk"),
        PAGE_VISIT              ("09",   "pv"),;

        private final String idx;  // hbase row key prefix
        private final String abbr;  // field name in profiling

        public String idx() {
            return idx;
        }

        public String abbr() {
            return abbr;
        }

        Event(String idx, String abbr) {
            this.idx = idx;
            this.abbr = abbr;
        }
    }

    /**
     * append only
     */
    public enum Field {
        UCID                ("000", "user_id",              "ucid"),
        UUID                ("001", "uuid",                 "uuid"),
        CITY_ID             ("002", "city_id",              "city_id"),
        CHANNEL_ID          ("003", "channel_id",           "ch_id"),
        DETAIL_ID           ("004", "detail_id",            "dtl_id"),
        DETAIL_TYPE         ("005", "detail_id_type",       "dtl_type"),
        KEYWORDS            ("006", "keywords",             "kws"),
        TIMESTAMP           ("007", "timestamp",            "ts"),
        LOCATION            ("008", "",                     "loc"),
        OS                  ("009", "os_type",              "os"),
        EVENTS              ("",    "events",               ""),
        EVENT_ID            ("00a", "event_id",             ""),
        EVENT_NAME          ("00b", "event_name",           "evt_name"),
        ATTENTION_ID        ("00c", "attention_id",         "fl_id"),
        ATTENTION_ACTION    ("00d", "attention_action",     "fl_act"),
        ATTENTION_TYPE      ("00e", "attention_type",       "fl_type"),
        SESSION_ID          ("00f", "session_id",           "ssn_id"),
        REFERER_PAGE        ("010", "referer_page",         "ref_pg"),
        CURRENT_PAGE        ("011", "current_page",         "cur_pg"),
        OUT_SOURCE_TYPE     ("012", "out_source_type",      "o_src_type"),
        OUT_SOURCE_WEBSITE  ("013", "out_source_website",   "o_src_site"),
        OUT_SOURCE_LINK_NO  ("014", "out_source_link_no",   "o_src_lk_no"),
        OUT_KEYWORDS        ("015", "out_keywords",         "o_kws"),
        IS_SEARCH           ("016", "is_search",            "is_srh"),
        SEARCH_SOURCE       ("017", "search_source",        "srh_src"),
        RESULT_NUMBER       ("018", "result_number",        "res_num"),
        PAGE_INDEX          ("019", "page_index",           "pg_idx"),
        SEARCH_POSITION     ("01a", "search_position",      "srh_pos"),
        USE_SUGGESTION      ("01b", "use_suggestion",       "use_sug"),
        SUGGESTION_POSITION ("01c", "sug_position",         "sug_pos"),

        DISTRICTS           ("01d", "districtId",           "ftr_dst_ids"),
        PRICE_MIN           ("01e", "priceTotalMin",        "ftr_prc_min"),
        PRICE_MAX           ("01f", "priceTotalMax",        "ftr_prc_max"),
        UNIT_PRICE_MIN      ("020", "priceUnitAvg",         "ftr_u_prc_min"),
        UNIT_PRICE_MAX      ("021", "priceUnitAvg",         "ftr_u_prc_max"),
        BUILDING_TYPES      ("022", "buildType",            "ftr_bld_types"),
        HOUSE_TYPES         ("023", "houseType",            "ftr_hou_types"),
        ORIENTATION         ("024", "orientation",          "ftr_ori_ids"),
        FEATURES            ("025", "feature",              "ftr_feats"),
        SELL_STATUS         ("026", "sellStatus",           "ftr_sel_status"),
        BIZCIRCLE_IDS       ("027", "bizcircleId",          "ftr_ccl_ids"),
        SUBWAY_LINE_IDS     ("028", "subwayLineId",         "ftr_sub_ids"),
        ROOM_NUM            ("029", "bedroomNum",           "ftr_room_num"),
        RENT_AREA_MIN       ("02a", "rentArea",             "ftr_rt_area_min"),
        RENT_AREA_MAX       ("02b", "rentArea",             "ftr_rt_area_max"),
        HOUSE_AREA_MIN      ("02c", "houseArea",            "ftr_hou_area_min"),
        HOUSE_AREA_MAX      ("02d", "houseArea",            "ftr_hou_area_max"),
        FLOOR_LEVEL         ("02e", "floorLevel",           "ftr_flr_lv"),
        COMMUNITY_IDS       ("02f", "communityId",          "ftr_comm_ids"),
        DECORATION          ("030", "decoration",           "ftr_deco"),
        HEATING_TYPE        ("031", "heatingType",          "ftr_heat_types"),
        TAGS                ("032", "tags",                 "ftr_tags"),

        CHANNEL             ("033", "channel",              "ftr_ch"),
        FILTER_CITY_ID      ("034", "city_id",              "ftr_city_id"),
        DISTRICT_ID         ("035", "district_id",          "ftr_dist_id"),
        MAX_PRICE           ("036", "max_price",            "ftr_max_prc"),
        MIN_PRICE           ("037", "min_price",            "ftr_min_prc"),
        MAX_AREA            ("038", "max_area",             "ftr_max_area"),
        MIN_AREA            ("039", "min_area",             "ftr_min_area"),
        ROOM_COUNT          ("040", "room_count",           "ftr_room_num"),
        FTR_SUBWAY_LINE_ID  ("041", "subway_line_id",       "ftr_sub_id"),
        SUBWAY_STATION_ID   ("042", "subway_station_id",    "ftr_sub_st_id"),
        QUERY_STR           ("043", "queryStr",             "ftr_qry"),
        IS_SUGGESTION       ("044", "is_suggestion",        "ftr_is_sug"),

        WRITE_TS            ("045", "",                     "w_ts"),
        IP                  ("046", "",                     "ip");

        private final String idx;
        private final String value; // field name in log message
        private final String abbr;  // field name in profiling, e.g. ES doc field, hbase table CQ.

        public String idx() {
            return idx;
        }

        /**
         * @return field name in log message
         */
        public String value() {
            return value;
        }

        public String abbr() {
            return abbr;
        }

        Field(String idx, String value, String abbr) {
            this.idx = idx;
            this.value = value;
            this.abbr = abbr;
        }
    }
}
