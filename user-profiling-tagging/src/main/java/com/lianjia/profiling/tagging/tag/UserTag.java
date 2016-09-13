package com.lianjia.profiling.tagging.tag;

import com.lianjia.profiling.tagging.features.Features;

/**
 * @author fenglei@lianjia.com on 2016-07
 */

/**
 * todo 阈值可以没菜请求用参数配置?
 */
public enum UserTag {

    PHONE("phone"),

    UCID("ucid"),

    UUID("uuid"),

    // 最后一次跟新prefer, decay不算
    WRITE_TS("write_ts"),

    ROOM_NUM_CNT("room_num_cnt"),
    ROOM_NUM("room_num", "卧室数", 3, 0.1, 0.1, ROOM_NUM_CNT, Features.RoomNum.values().length),

    AREA_CNT("area_cnt"),
    AREA("area", "面积", 3, 0.1, 0.1, AREA_CNT, Features.AreaRange.values().length),

    PRICE_LV1_CNT("price_lv1_cnt"),
    PRICE_LV2_CNT("price_lv2_cnt"),
    PRICE_LV3_CNT("price_lv3_cnt"),
    PRICE_LV1("price_lv1", "价格(一线城市)", 3, 0.1, 0.1, PRICE_LV1_CNT, Features.PriceRangeLv1.values().length),
    PRICE_LV2("price_lv2", "价格(准一线城市)", 3, 0.1, 0.1, PRICE_LV2_CNT, Features.PriceRangeLv2.values().length),
    PRICE_LV3("price_lv3", "价格(二线城市)", 3, 0.1, 0.1, PRICE_LV3_CNT, Features.PriceRangeLv3.values().length),

    FLOOR_CNT("floor_cnt"),
    FLOOR("floor", "楼层", 3, 0.1, 0.1, FLOOR_CNT, Features.RelativeFloorLevel.values().length),

    ORIENT_CNT("orient_cnt"),
    ORIENT("orient", "朝向", 3, 0.1, 0.1, ORIENT_CNT, Features.Orientation.values().length),

    BUILDING_AGE_CNT("bd_age_cnt"),
    BUILDING_AGE("bd_age", "楼龄", 3, 0.1, 0.1, BUILDING_AGE_CNT, Features.BuildingAge.values().length),

    DISTRICT_CNT("district_cbf"),
    DISTRICT("district", "行政区", 5, 0.1, 0.1, DISTRICT_CNT, 10),

    BIZCIRCLE_CNT("bizcircle_cbf"),
    BIZCIRCLE("bizcircle", "商圈", 5, 0.1, 0.05, BIZCIRCLE_CNT, 50),

    RESBLOCK_CNT("resblock_cbf"),
    RESBLOCK("resblock", "小区", 10, 0.1, 0.01, RESBLOCK_CNT, 300),

    UNIQUE_CNT("unique_cnt"),
    UNIQUE("unique", "唯一", 1, 0.1, 0.6, UNIQUE_CNT, 2),
    SCHOOL_CNT("school_cnt"),

    SCHOOL("school", "学区房", 1, 0.1, 0.6, SCHOOL_CNT, 2),

    METRO_CNT("metro_cnt"),
    METRO("metro", "地铁房", 1, 0.1, 0.6, METRO_CNT, 2);

    String val;
    String repr;
    int topNum = -1;
    double minSupport = -1;
    double percent = -1;
    UserTag counter;
    int size = -1;

    public String val() {
        return val;
    }

    public String repr() {
        return repr;
    }

    public double minSupport() {
        return minSupport;
    }

    public int topNum() {
        return topNum;
    }

    public double percent() {
        return percent;
    }

    public int size() {
        return size;
    }

    public UserTag counter() {
        return counter;
    }

    UserTag(String val, String repr, int num, double minSupport, double percent, UserTag counter, int size) {
        this(val);
        this.repr = repr;
        this.topNum = num;
        this.minSupport = minSupport;
        this.percent = percent;
        this.counter = counter;
        this.size = size;
    }

    UserTag(String val) {
        this.val = val;
    }
}
