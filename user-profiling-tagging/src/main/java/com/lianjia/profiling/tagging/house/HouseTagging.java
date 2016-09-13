package com.lianjia.profiling.tagging.house;

import com.alibaba.fastjson.JSONObject;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.tagging.features.HouseProperties;
import com.lianjia.profiling.tagging.tag.HouseTag;
import com.lianjia.profiling.util.FieldUtil;
import org.apache.hadoop.hbase.shaded.org.mortbay.util.ajax.JSON;

import java.util.HashMap;
import java.util.Map;

import static com.lianjia.profiling.tagging.features.Features.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class HouseTagging {
    private static final Logger LOG = LoggerFactory.getLogger(HouseTagging.class.getName());

    private static void setPrice(HouseProperties prop, Map<String, Object> house) {
        if (house.containsKey("city_id") && house.containsKey("list_price")) {
            String cityId = house.get("city_id").toString();
            int idx = getPriceIdx(house.get("city_id").toString(),
                                  Double.parseDouble(house.get("list_price").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, city_id: %s, list_price: %s", house.get("city_id").toString(),
                //                        house.get("list_price").toString()));
            } else {
                Class<?> range = PRICE_MAPPING.get(cityId);
                if (PriceRangeLv1.class.isAssignableFrom(range)) prop.update(HouseTag.PRICE_LV1, idx);
                else if (PriceRangeLv2.class.isAssignableFrom(range))
                    prop.update(HouseTag.PRICE_LV2, idx);
                else if (PriceRangeLv3.class.isAssignableFrom(range))
                    prop.update(HouseTag.PRICE_LV3, idx);
                else {
                    throw new IllegalStateException("unmatched price range, city id: " + cityId);
                }
            }
        }
    }

    protected static void setArea(HouseProperties prop, Map<String, Object> house) {
        if (house.containsKey("area")) {
            int idx = getAreaIdx(Double.parseDouble(house.get("area").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, area: %s", house.get("area").toString()));
            } else {
                prop.update(HouseTag.AREA, idx);
            }
        }
    }

    protected static void setFloor(HouseProperties prop, Map<String, Object> house) {
        if (house.containsKey("floor_name") && house.containsKey("floors_num")) {
            int idx = getRelativeFloorLevel(Integer.parseInt(house.get("floor_name").toString()),
                                            Integer.parseInt(house.get("floors_num").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, floor_name: %s, floors_num: %s.", house.get("floor_name").toString(),
                //                        house.get("floors_num").toString()));
            } else {
                prop.update(HouseTag.FLOOR, idx);
            }
        }
    }

    protected static void setOrient(HouseProperties prop, Map<String, Object> house) {
        if (house.containsKey("orient_id")) {
            int idx = getOrientIdx(Long.parseLong(house.get("orient_id").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, orient_id: %s.", house.get("orient_id").toString()));
            } else {
                prop.update(HouseTag.ORIENT, idx);
            }
        }
    }

    protected static void setBuildingAge(HouseProperties prop, Map<String, Object> house) {
        if (house.containsKey("completed_year")) {
            int idx = getBuildingAgeIdx(getDate(house, new String[]{"creation_date"}),
                                        Integer.parseInt(house.get("completed_year").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, completed_year: %s.",
                //                        house.get("completed_year").toString()));
            } else {
                prop.update(HouseTag.BUILDING_AGE, idx);
            }
        }
    }

    protected static void setId(HouseProperties prop, Map<String, Object> event, String fieldName, HouseTag field) {
        if (event.containsKey(fieldName)) {
            prop.update(field, event.get(fieldName).toString());
        }
    }

    protected static void setBool(HouseProperties prop, Map<String, Object> event, String fieldName, HouseTag field) {
        if (event.containsKey(fieldName)) {
            String val = event.get(fieldName).toString();
            if (!isBoolValueValid(val)) {
                // LOG.warn(String.format("invalid arguments, %s: %s.", fieldName, event.get(fieldName).toString()));
            } else {
                if(!val.equals("0") && !val.equals("1")) {
                    // LOG.warn("invalid bool field: " + val);
                } else {
                    prop.update(field, val.equals("1"));
                }
            }
        }
    }

    protected static void setMetro(HouseProperties prop, Map<String, Object> event) {
        if (event.containsKey("metro_dist_name")) {
            int isMetro = isMetro(event.get("metro_dist_name").toString()); // todo: metro_dist_code
            if (isMetro == -1) {
                // LOG.warn(String.format("invalid arguments, metro_dist_name: %s.", event.get("metro_dist_name").toString()));
            } else {
                prop.update(HouseTag.METRO, isMetro);
            }
        }
    }

    protected static void setRoomNum(HouseProperties prop, Map<String, Object> event) {
        if (event.containsKey("bedr_num")) {
            int idx = getRoomNumIdx(Integer.parseInt(event.get("bedr_num").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, bedr_num: %s.", event.get("bedr_num").toString()));
            } else {
                prop.update(HouseTag.ROOM_NUM, idx);
            }
        }
    }

    protected static void setBizType(HouseProperties prop, Map<String, Object> event) {
        if (event.containsKey("biz_type")) {
            String type = event.get("biz_type").toString();
            if (!FieldUtil.isFieldValid(type)) {
                // LOG.warn(String.format("invalid arguments, biz_type: %s.", event.get("biz_type").toString()));
            } else {
                prop.update(HouseTag.BIZ_TYPE, type);
            }
        }
    }

    protected static void setStatus(HouseProperties prop, Map<String, Object> event) {
        if (event.containsKey("status")) {
            String status = event.get("status").toString();
            if (!FieldUtil.isFieldValid(status)) {
                // LOG.warn(String.format("invalid arguments, status: %s.", event.get("status").toString()));
            } else {
                prop.update(HouseTag.STATUS, status);
            }
        }
    }

    protected static void setMeta(HouseProperties prop, Map<String, Object> house, String fieldName, HouseTag field) {
        if (house.containsKey(fieldName)) prop.updateMeta(field, house.get(fieldName));
    }

    public static HouseProperties compute(String json) {
        Map<String, Object> house = new HashMap<>();
        for (Map.Entry<String, Object> e: ((JSONObject)JSON.parse(json)).entrySet())
            house.put(e.getKey(), e.getValue());
        return compute(house);
    }

    public static HouseProperties compute(Map<String, Object> house) {
        HouseProperties prop = new HouseProperties();

        setMeta(prop, house, "house_id", HouseTag.HOUSE_ID);
        setMeta(prop, house, "hdic_house_id", HouseTag.HDIC_HOUSE_ID);
        setMeta(prop, house, "city_id", HouseTag.CITY);
        setMeta(prop, house, "house_id", HouseTag.HOUSE_ID);

        setRoomNum(prop, house);
        setPrice(prop, house);
        setArea(prop, house);
        setFloor(prop, house);
        setOrient(prop, house);
        setBuildingAge(prop, house);
        setBizType(prop, house);
        setStatus(prop, house);

        setId(prop, house, "district_code", HouseTag.DISTRICT);
        setId(prop, house, "bizcircle_id", HouseTag.BIZCIRCLE);
        setId(prop, house, "resblock_id", HouseTag.RESBLOCK);

        setBool(prop, house, "is_school_district", HouseTag.SCHOOL);
        setBool(prop, house, "is_unique", HouseTag.UNIQUE);

        setMetro(prop, house);

        return prop;
    }
}
