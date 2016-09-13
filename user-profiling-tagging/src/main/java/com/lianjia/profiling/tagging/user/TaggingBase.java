package com.lianjia.profiling.tagging.user;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.tagging.features.Features;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.UserTag;

import java.util.Map;

import static com.lianjia.profiling.tagging.features.Features.*;

/**
 * @author fenglei@lianjia.com on 2016-07
 */

/**
 * 特征分为id型, 和category型
 *
 * @see <a href="http://www.tugberkugurlu.com/archive/elasticsearch-array-contains-search-with-terms-filter"/>
 */

public abstract class TaggingBase {
    private static final Logger LOG = LoggerFactory.getLogger(TaggingBase.class.getName());

    @SuppressWarnings("Duplicates")
    protected static void updatePrice(UserPreference prefer, Map<String, Object> event, String fieldName, float weight) {
        if (event.containsKey("city_id") && event.containsKey(fieldName)) {
            String cityId = event.get("city_id").toString();
            int idx = getPriceIdx(event.get("city_id").toString(),
                                  Double.parseDouble(event.get(fieldName).toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, city_id: %s, %s: %s", event.get("city_id").toString(), fieldName,
                //                       event.get(fieldName).toString()));
            } else {
                Class<?> range = PRICE_MAPPING.get(cityId);
                if (Features.PriceRangeLv1.class.isAssignableFrom(range)) prefer.update(UserTag.PRICE_LV1, idx, weight);
                else if (Features.PriceRangeLv2.class.isAssignableFrom(range))
                    prefer.update(UserTag.PRICE_LV2, idx, weight);
                else if (Features.PriceRangeLv3.class.isAssignableFrom(range))
                    prefer.update(UserTag.PRICE_LV3, idx, weight);
                else {
                    throw new IllegalStateException("unmatched price range, city id: " + cityId);
                }
            }
        }
    }

    protected static void updateArea(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("area")) {
            int idx = getAreaIdx(Double.parseDouble(event.get("area").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, area: %s", event.get("area").toString()));
            } else {
                prefer.update(UserTag.AREA, idx, weight);
            }
        }
    }

    protected static void updateFloor(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("floor_name") && event.containsKey("floors_num")) {
            int idx = getRelativeFloorLevel(Integer.parseInt(event.get("floor_name").toString()),
                                            Integer.parseInt(event.get("floors_num").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, floor_name: %s, floors_num: %s.", event.get("floor_name").toString(),
                //                        event.get("floors_num").toString()));
            } else {
                prefer.update(UserTag.FLOOR, idx, weight);
            }
        }
    }

    protected static void updateOrient(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("orient_id")) {
            int idx = getOrientIdx(Long.parseLong(event.get("orient_id").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, orient_id: %s.", event.get("orient_id").toString()));
            } else {
                prefer.update(UserTag.ORIENT, idx, weight);
            }
        }
    }

    protected static void updateRoomNum(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("room_num")) {
            int idx = getRoomNumIdx(Integer.parseInt(event.get("room_num").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, room_num: %s.", event.get("room_num").toString()));
            } else {
                prefer.update(UserTag.ROOM_NUM, idx, weight);
            }
        }
    }

    protected static void updateBedroomNum(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("bedr_num")) {
            int idx = getRoomNumIdx(Integer.parseInt(event.get("bedr_num").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, bedr_num: %s.", event.get("bedr_num").toString()));
            } else {
                prefer.update(UserTag.ROOM_NUM, idx, weight);
            }
        }
    }

    protected static void updateBuildingAge(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("completed_year")) {
            int idx = getBuildingAgeIdx(getDate(event, new String[]{"creation_date"}),
                                        Integer.parseInt(event.get("completed_year").toString()));
            if (idx == -1) {
                // LOG.warn(String.format("invalid arguments, completed_year: %s.", event.get("completed_year").toString()));
            } else {
                prefer.update(UserTag.BUILDING_AGE, idx, weight);
            }
        }
    }

    protected static void updateId(UserPreference prefer, Map<String, Object> event, String fieldName, UserTag field, float weight) {
        if (event.containsKey(fieldName)) {
            prefer.update(field, event.get(fieldName), weight);
        }
    }

    protected static void updateBool(UserPreference prefer, Map<String, Object> event, String fieldName, UserTag field, float weight) {
        if (event.containsKey(fieldName)) {
            String val = event.get(fieldName).toString();
            if (!isBoolValueValid(val)) {
                // LOG.warn(String.format("invalid arguments, %s: %s.", fieldName, event.get(fieldName).toString()));
            } else {
                prefer.update(field, event.get(fieldName), weight);
            }
        }
    }

    protected static void updateMetro(UserPreference prefer, Map<String, Object> event, float weight) {
        if (event.containsKey("metro_dist_name")) {
            int isMetro = isMetro(event.get("metro_dist_name").toString()); // todo: metro_dist_code
            if (isMetro == -1) {
                // LOG.warn(String.format("invalid arguments, metro_dist_name: %s.", event.get("metro_dist_name").toString()));
            } else {
                prefer.update(UserTag.METRO, isMetro, weight);
            }
        }
    }

    protected static void updateMeta(UserPreference prefer, UserTag key, Object value) {
        prefer.updateMeta(key, value);
    }
}
