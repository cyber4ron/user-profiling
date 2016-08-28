package com.lianjia.profiling.tagging.user;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.UserTag;

import java.util.List;
import java.util.Map;

import static com.lianjia.profiling.tagging.features.Features.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class OnlineEventTagging extends TaggingBase {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineEventTagging.class.getName());

    public static void tagByHouse(UserPreference prefer, Map<String, Object> house, double weight) {

        updatePrice(prefer, house, "list_price", weight);
        updateArea(prefer, house, weight);
        updateFloor(prefer, house, weight);
        updateBuildingAge(prefer, house, weight);
        updateOrient(prefer, house, weight);
        updateBedroomNum(prefer, house, weight);

        updateId(prefer, house, "district_code", UserTag.DISTRICT, weight);
        updateId(prefer, house, "bizcircle_id", UserTag.BIZCIRCLE, weight);
        updateId(prefer, house, "resblock_id", UserTag.RESBLOCK, weight);

        updateBool(prefer, house, "is_school_district", UserTag.SCHOOL, weight);
        updateBool(prefer, house, "is_unique", UserTag.UNIQUE, weight);

        updateMetro(prefer, house, weight);
    }

    @SuppressWarnings({"unchecked", "Duplicates"})
    public static void onDetail(UserPreference prefer, Map<String, Object> event, EventType type) {
        if(!event.get("dtl_type").toString().equals("1")
                || !event.containsKey("house")
                || !event.containsKey("ts")) {
            // LOG.warn("invalid event", event);
            return;
        }

        try {
            Map<String, Object> house = (Map<String, Object>) event.get("house");

            long ts = Long.parseLong(event.get("ts").toString());
            if (ts > System.currentTimeMillis() + 86400000) return;

            int dateDiff = (int) ((ts - System.currentTimeMillis()) / 86400000);
            if (!(-MAX_DECAY_DAYS <= dateDiff && dateDiff <= 0)) return;

            double weight = WEIGHTS.get(type) * DECAY.get(dateDiff);

            tagByHouse(prefer, house, weight);

        } catch (Exception ex) {
            LOG.warn("", ex);
        }
    }

    @SuppressWarnings({"unchecked", "Duplicates"})
    public static void onFollow(UserPreference prefer, Map<String, Object> event, EventType type) {
        if(!event.get("fl_type").toString().equals("2")
                || !event.containsKey("house")
                || !event.containsKey("ts")) {
            // LOG.warn("invalid event", event);
            return;
        }

        try {
            Map<String, Object> house = (Map<String, Object>) event.get("house");

            long ts = Long.parseLong(event.get("ts").toString());
            if(ts > System.currentTimeMillis() + 86400000) return;

            int dateDiff = (int)((ts - System.currentTimeMillis()) / 86400000);
            if (!(-MAX_DECAY_DAYS <= dateDiff && dateDiff <= 0)) return;

            double weight = WEIGHTS.get(type) * DECAY.get(dateDiff);

            tagByHouse(prefer ,house , weight);

        } catch (Exception ex) {
            LOG.warn("", ex);
        }
    }

    @SuppressWarnings("Duplicates")
    public static UserPreference compute(UserTag idType, String id, List<Map<String, Object>> events) {
        UserPreference prefer = new UserPreference();
        prefer.updateMeta(idType, id);

        for (Map<String, Object> event : events) {
            if(event.containsKey("evt") && event.get("evt").equals("dtl")) {
                onDetail(prefer, event, EventType.PC_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("fl")) {
                onFollow(prefer, event, EventType.PC_FOLLOW);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_dtl")) {
                onDetail(prefer, event, EventType.MOBILE_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_fl")) {
                onFollow(prefer, event, EventType.MOBILE_FOLLOW);
            }
        }

        return prefer;
    }

    @SuppressWarnings("Duplicates")
    public static UserPreference computeParallel(UserTag idType, String id, List<Map<String, Object>> events) {
        UserPreference prefer = new UserPreference();
        prefer.updateMeta(idType, id);

        for (Map<String, Object> event : events) {
            if(event.containsKey("evt") && event.get("evt").equals("dtl")) {
                onDetail(prefer, event, EventType.PC_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("fl")) {
                onFollow(prefer, event, EventType.PC_FOLLOW);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_dtl")) {
                onDetail(prefer, event, EventType.MOBILE_DETAIL);
            } else if(event.containsKey("evt") && event.get("evt").equals("mob_fl")) {
                onFollow(prefer, event, EventType.MOBILE_FOLLOW);
            }
        }

        return prefer;
    }
}
