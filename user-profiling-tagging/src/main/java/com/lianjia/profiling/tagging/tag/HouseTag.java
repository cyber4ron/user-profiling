package com.lianjia.profiling.tagging.tag;

/**
 * @author fenglei@lianjia.com on 2016-07
 */

public enum HouseTag {
    HOUSE_ID        ("house_id"),
    HDIC_HOUSE_ID   ("hdic_house_id"),
    CITY            ("city_id"),
    WRITE_TS        ("write_ts"),
    ROOM_NUM        ("room_num", UserTag.ROOM_NUM, 1.5F),
    AREA            ("area", UserTag.AREA, 1.5F),
    PRICE_LV1       ("price_lv1", UserTag.PRICE_LV1, 2F),
    PRICE_LV2       ("price_lv2", UserTag.PRICE_LV2, 2F),
    PRICE_LV3       ("price_lv3", UserTag.PRICE_LV3, 2F),
    FLOOR           ("floor", UserTag.FLOOR, 1),
    ORIENT          ("orient", UserTag.ORIENT, 1.5F),
    BUILDING_AGE    ("bd_age", UserTag.BUILDING_AGE, 0),
    DISTRICT        ("district", UserTag.DISTRICT, 2),
    BIZCIRCLE       ("bizcircle", UserTag.BIZCIRCLE, 1),
    RESBLOCK        ("resblock", UserTag.RESBLOCK, 1),
    UNIQUE          ("unique", UserTag.UNIQUE, 1),
    SCHOOL          ("school", UserTag.SCHOOL, 1),
    METRO           ("metro", UserTag.METRO, 1),
    BIZ_TYPE        ("biz_type"),
    STATUS          ("status");

    String val;
    UserTag tag;
    float boost;

    HouseTag(String val) {
        this.val = val;
    }

    HouseTag(String val, UserTag tag, float boost) {
        this(val);
        this.tag = tag;
        this.boost = boost;
    }

    public UserTag userTag() {
        return tag;
    }

    public String val() {
        return val;
    }

    public float boost() {
        return boost;
    }
}
