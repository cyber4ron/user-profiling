package com.lianjia.profiling.tagging.user;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.tagging.features.Features.*;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.UserTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.lianjia.profiling.tagging.features.Features.*;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class OfflineEventTagging extends TaggingBase {
    private static final Logger LOG = LoggerFactory.getLogger(OfflineEventTagging.class.getName());

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getOfflineDelegation(Map<String, Object> offline) {
        try {
            List<Map<String, Object>> delegations = new ArrayList<>();
            JSONArray delegationArr = (JSONArray) (offline.get("delegations"));
            for (Object del : delegationArr) {
                delegations.add((Map<String, Object>) del);
            }
            return delegations;
        } catch (Exception ex) {
            LOG.warn("", ex);
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getOfflineTouring(Map<String, Object> offline) {
        try {
            List<Map<String, Object>> touringHouses = new ArrayList<>();
            JSONArray tourings = (JSONArray) (offline.get("tourings"));
            for (Object touring : tourings) {
                if (((JSONObject) touring).containsKey("houses"))
                    for (Object house : ((JSONArray) ((JSONObject) touring).get("houses"))) {
                        touringHouses.add((Map<String, Object>) house);
                    }
            }
            return touringHouses;
        } catch (Exception ex) {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getOfflineContract(Map<String, Object> offline) {
        try {
            List<Map<String, Object>> contracts = new ArrayList<>();
            JSONArray contractArr = (JSONArray) (offline.get("contract"));
            for (Object del : contractArr) {
                contracts.add((Map<String, Object>) del);
            }
            return contracts;
        } catch (Exception ex) {
            return Collections.emptyList();
        }
    }

    /**
     * compute preference
     * todo: 验证!=-1
     * todo: int / long
     * todo: 商圈 -> bizcircle_id
     * todo: 处理idx返回-1
     */
    @SuppressWarnings("Duplicates")
    public static UserPreference compute(Map<String, Object> offline) {

        UserPreference prefer = new UserPreference();

        if (offline.containsKey("phone")) {
            prefer.updateMeta(UserTag.PHONE, offline.get("phone").toString());
        }

        for (Map<String, Object> del : getOfflineDelegation(offline)) {

            int dateDiff = getDateDiff(del, new String[]{"creation_time", "update_time"});
            if (!(-MAX_DECAY_DAYS <= dateDiff && dateDiff <= 0)) continue;

            if (del.containsKey("city_id") && del.containsKey("price_min") && del.containsKey("price_max")) {
                int[] indices = getPriceIdx(del.get("city_id").toString(),
                                            Double.parseDouble(del.get("price_min").toString()),
                                            Double.parseDouble(del.get("price_max").toString()));

                String cityId = del.get("city_id").toString();
                for (int idx : indices) {
                    if (idx == -1) {
                        LOG.warn(String.format("invalid arguments, city_id: %s, price_min: %s, price_max: %s.", del.get("city_id").toString(),
                                               del.get("price_min").toString(), del.get("price_max").toString()));
                        continue;
                    }

                    Class<?> range = PRICE_MAPPING.get(cityId);
                    double weight = WEIGHTS.get(EventType.DELEGATION) * DECAY.get(dateDiff);

                    if (PriceRangeLv1.class.isAssignableFrom(range))
                        prefer.update(UserTag.PRICE_LV1, idx, weight);
                    else if (PriceRangeLv2.class.isAssignableFrom(range))
                        prefer.update(UserTag.PRICE_LV2, idx, weight);
                    else if (PriceRangeLv3.class.isAssignableFrom(range))
                        prefer.update(UserTag.PRICE_LV3, idx, weight);
                    else {
                        throw new IllegalStateException("unmatched price range, city id: " + cityId);
                    }
                }
            }

            if (del.containsKey("area_min") && del.containsKey("area_max")) {
                int[] indices = getAreaIdx(Double.parseDouble(del.get("area_min").toString()),
                                           Double.parseDouble(del.get("area_max").toString()));

                for (int idx : indices) {
                    if (idx == -1) {
                        LOG.warn(String.format("invalid arguments, area_min: %s, area_max: %s.", del.get("area_min").toString(),
                                               del.get("area_max").toString()));
                        continue;
                    }
                    prefer.update(UserTag.AREA, idx,
                                  WEIGHTS.get(EventType.DELEGATION) * DECAY.get(dateDiff));
                }
            }

            if (del.containsKey("room_min") && del.containsKey("room_max")) {
                int[] indices = getRoomNumIdx(Integer.parseInt(del.get("room_min").toString()),
                                              Integer.parseInt(del.get("room_max").toString()));

                for (int idx : indices) {
                    if (idx == -1) {
                        LOG.warn(String.format("invalid arguments, room_min: %s, room_max: %s.", del.get("room_min").toString(),
                                               del.get("room_max").toString()));
                        continue;
                    }
                    prefer.update(UserTag.ROOM_NUM, idx,
                                  WEIGHTS.get(EventType.DELEGATION) * DECAY.get(dateDiff));
                }
            }
        }

        for (Map<String, Object> house : getOfflineTouring(offline)) {

            int dateDiff = getDateDiff(house, new String[]{"creation_date"});
            if (!(-MAX_DECAY_DAYS <= dateDiff && dateDiff <= 0)) continue;

            double weight = WEIGHTS.get(EventType.TOURING) * DECAY.get(dateDiff);

            updatePrice(prefer, house, "list_price", weight);
            updateArea(prefer, house, weight);
            updateFloor(prefer, house, weight);
            updateBuildingAge(prefer, house, weight);
            updateOrient(prefer, house, weight);
            updateRoomNum(prefer, house, weight);

            updateId(prefer, house, "district_code", UserTag.DISTRICT, weight);
            updateId(prefer, house, "bizcircle_id", UserTag.BIZCIRCLE, weight);
            updateId(prefer, house, "resblock_id", UserTag.RESBLOCK, weight);

            updateBool(prefer, house, "is_school_district", UserTag.SCHOOL, weight);
            updateBool(prefer, house, "is_unique", UserTag.UNIQUE, weight);

            updateMetro(prefer, house, weight);
        }

        for (Map<String, Object> contract : getOfflineContract(offline)) {

            int dateDiff = getDateDiff(contract, new String[]{"created_time"});
            if (!(-MAX_DECAY_DAYS <= dateDiff && dateDiff <= 0)) continue;

            double weight = WEIGHTS.get(EventType.TOURING) * DECAY.get(dateDiff);

            updatePrice(prefer, contract, "price", weight);
            updateArea(prefer, contract, weight);
            updateFloor(prefer, contract, weight);
            updateBuildingAge(prefer, contract, weight);
            updateOrient(prefer, contract, weight);
            updateRoomNum(prefer, contract, weight);

            updateId(prefer, contract, "district_code", UserTag.DISTRICT, weight);
            updateId(prefer, contract, "bizcircle_id", UserTag.BIZCIRCLE, weight);
            updateId(prefer, contract, "resblock_id", UserTag.RESBLOCK, weight);

            updateBool(prefer, contract, "is_school_district", UserTag.SCHOOL, weight);
            updateBool(prefer, contract, "is_unique", UserTag.UNIQUE, weight);

            updateMetro(prefer, contract, weight);
        }

        return prefer;
    }
}
