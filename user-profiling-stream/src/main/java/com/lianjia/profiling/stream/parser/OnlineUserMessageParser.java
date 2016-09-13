package com.lianjia.profiling.stream.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.stream.MessageUtil;
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder;
import com.lianjia.profiling.stream.builder.OnlineUserEvents.Event;
import com.lianjia.profiling.stream.builder.OnlineUserEvents.Field;
import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.FieldUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lianjia.profiling.stream.builder.OnlineUserEventBuilder.*;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class OnlineUserMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineUserMessageParser.class.getName(), new HashMap<String, String>());

    public static void parse(String line,
                             List<Doc> userDocs,
                             List<EventDoc> eventDocs,
                             List<Row> eventsHbase,
                             List<Row> eventsIndicesHbase,
                             List<Object[]> redisKVs,
                             String indexName) {
        String[] parts = MessageUtil.extract(line);
        if (parts.length != 4) return;

        String json = MessageUtil.removeQuotes(MessageUtil.unescape(parts[1]));
        if (!json.endsWith("}")) return; // 少量

        parse(parts[0], json, parts[2], parts[3], userDocs, eventDocs, eventsHbase,
              eventsIndicesHbase, redisKVs, indexName);
    }

    public static boolean isValid(String id) {
        return id != null && !id.equals("") && !id.equals("undefined") && !id.equals("null");
    }

    protected static void parseSearchFilterItems(JSONObject json) {
    }

    protected static void parse(String tag, String json, String dateTime, String ip,
                                List<Doc> userDocs,
                                List<EventDoc> eventDocs,
                                List<Row> eventsHbase,
                                List<Row> eventsIndicesHbase,
                                List<Object[]> redisKVs,
                                String indexName) {
        Map<String, Object> message;
        try {
            message = JSON.parseObject(json);
        } catch (Exception ex) {
            return;
        }

        OnlineUserEventBuilder eb = OnlineUserEventBuilder.onNewMessage();

        // get ucid
        String ucid = null;
        if (message.containsKey(Field.UCID.value())) {
            ucid = FieldUtil.parseUserId(message.get(Field.UCID.value()));
        }

        // web端日志量有点大, 过滤掉user_id字段无效的
        // if (!tag.equals("[bigdata.mobile]") && !isValid(ucid)) return;

        // get uuid
        String uuid = null;
        if (message.containsKey(Field.UUID.value())) {
            uuid = (String) message.get(Field.UUID.value());
        }

        // 过滤掉两个id都无效的
        // if (!isValid(ucid) && !isValid(uuid)) return;

        // 优先用user_id作为id
        String id = isValid(ucid) ? ucid : uuid;

        // location
        String location = "";
        if (message.containsKey("latitude") && message.containsKey("longitude")) {
            location = message.get("latitude") + "," + message.get("longitude");
        }

        // get timestamp
        long ts = 0;
        Long tmp;

        if (message.containsKey(Field.TIMESTAMP.value())
                && (tmp = FieldUtil.parseTsMs(message.get(Field.TIMESTAMP.value()).toString())) != null) {
            // 移动端message有timestamp字段, 精度为sec
            ts = tmp;
        } else if ((tmp = DateUtil.datetimeToTimestampMs(dateTime)) != null) {
            // web端没有timestamp字段, 用message head上的, 精度为sec
            ts = tmp;
        }

        // 到此ts有可能为0或milli

        // get os type
        String osType = "";
        if (message.containsKey(Field.OS.value()))
            osType = message.get(Field.OS.value()).toString();

        // add ucid -> uuid mapping
        String _ucid = isValid(ucid) ? ucid : "";
        String _uuid = isValid(uuid) ? uuid : "";
        if (tag.equals("[bigdata.mobile]") && (isValid(ucid) || isValid(uuid))) {
            // mobile user
            eb.addUser(indexName, Event.MOBILE_USER.abbr(), DigestUtils.md5Hex(_ucid + _uuid).substring(0, 16))
                    .addUserField(Field.UCID, _ucid)
                    .addUserField(Field.UUID, _uuid)
                    .addUserField(Field.TIMESTAMP, ts)
                    .addUserField(Field.IP, ip)
                    .addUserField(Field.WRITE_TS, System.currentTimeMillis());
        } else if (isValid(ucid) || isValid(uuid)) {
            // web user
            eb.addUser(indexName, Event.USER.abbr(), DigestUtils.md5Hex(_ucid + _uuid).substring(0, 16))
                    .addUserField(Field.UCID, _ucid)
                    .addUserField(Field.UUID, _uuid)
                    .addUserField(Field.TIMESTAMP, ts)
                    .addUserField(Field.IP, ip)
                    .addUserField(Field.WRITE_TS, System.currentTimeMillis());
        }

        // message hashcode
        long messageHash = json.hashCode();

        switch (tag) {
            case "[bigdata.mobile]":
                Map<Field, Object> basicInfo = new HashMap<>();
                basicInfo.put(Field.UCID, ucid);
                basicInfo.put(Field.UUID, uuid);
                basicInfo.put(Field.IP, ip);
                basicInfo.put(Field.TIMESTAMP, ts);
                basicInfo.put(Field.WRITE_TS, System.currentTimeMillis());
                basicInfo.put(Field.LOCATION, location);
                basicInfo.put(Field.OS, osType);
                parseMobile(eb, message, id, ts, messageHash, basicInfo, indexName, redisKVs);
                break;

            // 可细分为详情页和搜索页访问, 所以注掉.
            // case "[bigdata.pagevisit]":
            //     eb.addEvent(indexName, Event.PAGE_VISIT, id, ColFam.WEB, Event.PAGE_VISIT, ts, messageHash)
            //             .addEventField(message, Field.CITY_ID)
            //             .addEventField(message, Field.CHANNEL_ID)
            //             .addEventField(message, Field.REFERER_PAGE)
            //             .addEventField(message, Field.CURRENT_PAGE)
            //             .addEventField(Field.UCID, ucid)
            //             .addEventField(Field.UUID, uuid)
            //             .addEventField(Field.TIMESTAMP, ts);
            //     break;

            case "[bigdata.detail]":
                eb.addEvent(indexName, Event.DETAIL, id, ColFam.WEB, Event.DETAIL, ts, messageHash)
                        .addEventField(Field.UCID, ucid)
                        .addEventField(Field.UUID, uuid)
                        .addEventField(Field.IP, ip)
                        .addEventField(Field.TIMESTAMP, ts)
                        .addEventField(Field.WRITE_TS, System.currentTimeMillis())
                        .addEventField(message, Field.SESSION_ID)
                        .addEventField(message, Field.CITY_ID)
                        .addEventField(message, Field.CHANNEL_ID)
                        .addEventField(message, Field.DETAIL_ID)
                        .addEventField(message, Field.DETAIL_TYPE)
                        .addEventField(message, Field.REFERER_PAGE)
                        .addEventField(message, Field.CURRENT_PAGE)
                        .addEventField(message, Field.IS_SEARCH)
                        .addEventField(message, Field.KEYWORDS)
                        .addEventField(message, Field.OUT_SOURCE_TYPE)
                        .addEventField(message, Field.OUT_SOURCE_WEBSITE)
                        .addEventField(message, Field.OUT_KEYWORDS)
                        .addEventField(message, Field.OUT_SOURCE_LINK_NO);
                if (message.containsKey(Field.ATTENTION_ID.value())) redisKVs.add(new Object[]{"dtl", message.get(Field.ATTENTION_ID.value()),
                        DateUtil.toFormattedDate(DateUtil.parseDateTime(ts))});
                break;

            case "[bigdata.search]":
                eb.addEvent(indexName, Event.SEARCH, id, ColFam.WEB, Event.SEARCH, ts, messageHash)
                        .addEventField(Field.UCID, ucid)
                        .addEventField(Field.UUID, uuid)
                        .addEventField(Field.IP, ip)
                        .addEventField(Field.TIMESTAMP, ts)
                        .addEventField(Field.WRITE_TS, System.currentTimeMillis())
                        .addEventField(message, Field.SESSION_ID)
                        .addEventField(message, Field.CITY_ID)
                        .addEventField(message, Field.CHANNEL_ID)
                        .addEventField(message, Field.OUT_SOURCE_TYPE)
                        .addEventField(message, Field.OUT_SOURCE_WEBSITE)
                        .addEventField(message, Field.OUT_KEYWORDS)
                        .addEventField(message, Field.KEYWORDS)
                        .addEventField(message, Field.SEARCH_SOURCE)
                        .addEventField(message, Field.RESULT_NUMBER)
                        .addEventField(message, Field.PAGE_INDEX);

                if (message.containsKey("filter_items")) {
                    JSONObject filter = null;
                    try {
                        filter = (JSONObject) message.get("filter_items");
                    } catch (ClassCastException ex) {
                        LOG.info("cast ex in get(\"filter_items\").");
                    }

                    if (filter == null) break;

                    eb.addEventField(Field.DISTRICTS, getJoinedArray(filter, Field.DISTRICTS));
                    eb.addEventField(Field.BIZCIRCLE_IDS, getJoinedArray(filter, Field.BIZCIRCLE_IDS));
                    eb.addEventField(Field.COMMUNITY_IDS, getJoinedArray(filter, Field.COMMUNITY_IDS));
                    eb.addEventField(Field.SUBWAY_LINE_IDS, getJoinedArray(filter, Field.SUBWAY_LINE_IDS));
                    eb.addEventField(Field.BUILDING_TYPES, getJoinedArray(filter, Field.BUILDING_TYPES));
                    eb.addEventField(Field.FLOOR_LEVEL, getJoinedArray(filter, Field.FLOOR_LEVEL));
                    eb.addEventField(Field.ORIENTATION, getJoinedArray(filter, Field.ORIENTATION));
                    eb.addEventField(Field.ROOM_NUM, getJoinedArray(filter, Field.ROOM_NUM));
                    eb.addEventField(Field.FEATURES, getJoinedArray(filter, Field.FEATURES));
                    eb.addEventField(Field.SELL_STATUS, getJoinedArray(filter, Field.SELL_STATUS));
                    eb.addEventField(Field.DECORATION, getJoinedArray(filter, Field.DECORATION));

                    // eb.addEventField(Field.HOUSE_TYPES, getJoinedNestedArray(filter, Field.HOUSE_TYPES)); 貌似区分度不高
                    eb.addEventField(Field.HEATING_TYPE, getJoinedNestedArray(filter, Field.HEATING_TYPE));
                    eb.addEventField(Field.TAGS, getJoinedNestedArray(filter, Field.TAGS));

                    if (filter.containsKey("priceTotalMin")) {
                        JSONObject priceTotalMin = ((JSONObject) filter.get("priceTotalMin"));
                        eb.addEventField(Field.PRICE_MAX, priceTotalMin.get("\u0000*\u0000_max"));
                    }

                    if (filter.containsKey("priceTotalMax")) {
                        JSONObject priceTotalMax = ((JSONObject) filter.get("priceTotalMax"));
                        eb.addEventField(Field.PRICE_MIN, priceTotalMax.get("\u0000*\u0000_min"));
                    }

                    if (filter.containsKey("priceTotal")) {
                        JSONObject priceTotal = ((JSONObject) filter.get("priceTotal"));
                        eb.addEventField(Field.PRICE_MIN, priceTotal.get("min"));
                        eb.addEventField(Field.PRICE_MAX, priceTotal.get("max"));
                    }

                    if (filter.containsKey("priceUnitAvg")) {
                        JSONObject rentArea = ((JSONObject) filter.get("priceUnitAvg"));
                        if (!rentArea.get("min").equals("*"))
                            eb.addEventField(Field.UNIT_PRICE_MIN, rentArea.get("min"));
                        if (!rentArea.get("max").equals("*"))
                            eb.addEventField(Field.UNIT_PRICE_MAX, rentArea.get("max"));
                    }

                    if (filter.containsKey("houseArea")) {
                        JSONObject houseArea = ((JSONObject) filter.get("houseArea"));
                        if (!houseArea.get("min").equals("*"))
                            eb.addEventField(Field.HOUSE_AREA_MIN, houseArea.get("min"));
                        if (!houseArea.get("max").equals("*"))
                            eb.addEventField(Field.HOUSE_AREA_MAX, houseArea.get("max"));
                    }

                    if (filter.containsKey("rentArea")) {
                        JSONObject rentArea = ((JSONObject) filter.get("rentArea"));
                        if (!rentArea.get("min").equals("*"))
                            eb.addEventField(Field.RENT_AREA_MIN, rentArea.get("min"));
                        if (!rentArea.get("max").equals("*"))
                            eb.addEventField(Field.RENT_AREA_MAX, rentArea.get("max"));
                    }

                }
                break;

            case "[bigdata.follow]":
                eb.addEvent(indexName, Event.FOLLOW, id, ColFam.WEB, Event.FOLLOW, ts, messageHash)
                        .addEventField(Field.UCID, ucid)
                        .addEventField(Field.UUID, uuid)
                        .addEventField(Field.IP, ip)
                        .addEventField(Field.TIMESTAMP, ts)
                        .addEventField(Field.WRITE_TS, System.currentTimeMillis())
                        .addEventField(message, Field.SESSION_ID)
                        .addEventField(message, Field.ATTENTION_ID)
                        .addEventField(message, Field.ATTENTION_TYPE);
                if (message.containsKey(Field.ATTENTION_ID.value())) redisKVs.add(new Object[]{"fl", message.get(Field.ATTENTION_ID.value()),
                        DateUtil.toFormattedDate(DateUtil.parseDateTime(ts))});
                break;

            default:
                break;
        }

        userDocs.addAll(eb.getUserDoc());
        eventDocs.addAll(eb.getEventDocs());
        eventsHbase.addAll(eb.getEventsToHbase());
        eventsIndicesHbase.addAll(eb.getEventIndexDocs());
    }

    private static String getJoinedArray(JSONObject json, Field field) {
        try {
            if (json.containsKey(field.value())) {
                JSONArray arr = ((JSONArray) json.get(field.value()));
                return StringUtils.join(arr.toArray(new Object[arr.size()]), ',');
            }
        } catch (ClassCastException ex) {
            LOG.info("cast ex in getJoinedArray.");
        }

        return null;
    }

    private static String getJoinedNestedArray(JSONObject json, Field field) {
        try {
            if (json.containsKey(field.value())) {
                JSONArray arr = (JSONArray) ((JSONObject) json.get(field.value())).get("arr");
                return StringUtils.join(arr.toArray(new Object[arr.size()]), ',');
            }
        } catch (ClassCastException ex) {
            LOG.info("cast ex in getJoinedNestedArray.");
        }

        return null;
    }

    private static void addMobileFilterItems(Map<String, Object> event, OnlineUserEventBuilder eb) {
        if (event.containsKey("filter_items")) {

            JSONObject filter = null;
            try {
                filter = (JSONObject) event.get("filter_items");
            } catch (ClassCastException ex) {
                LOG.info("cast ex in mobile get(\"filter_items\").");
            }

            if (filter == null) return;

            eb.addEventField(filter, Field.CHANNEL)
                    .addEventField(filter, Field.FILTER_CITY_ID)
                    .addEventField(filter, Field.DISTRICT_ID)
                    .addEventField(filter, Field.MAX_PRICE)
                    .addEventField(filter, Field.MIN_PRICE)
                    .addEventField(filter, Field.MAX_AREA)
                    .addEventField(filter, Field.MIN_AREA)
                    .addEventField(filter, Field.ROOM_COUNT)
                    .addEventField(filter, Field.FTR_SUBWAY_LINE_ID)
                    .addEventField(filter, Field.SUBWAY_STATION_ID)
                    .addEventField(filter, Field.QUERY_STR)
                    .addEventField(filter, Field.IS_SUGGESTION);
        }
    }

    @SuppressWarnings("unchecked")
    protected static void parseMobile(OnlineUserEventBuilder eb, Map<String, Object> message, String id, long ts, long messageHash,
                                      Map<Field, Object> basicInfo, String indexName, List<Object[]> redisKVs) {
        if (!message.containsKey(Field.EVENTS.value())) return;
        List<Map<String, Object>> events = (List<Map<String, Object>>) message.get(Field.EVENTS.value());

        for (Map<String, Object> event : events) {
            if (!event.containsKey(Field.EVENT_ID.value())) continue;
            switch ((int) event.get(Field.EVENT_ID.value())) {
                case 1: // detail
                    eb.addEvent(indexName, Event.MOBILE_DETAIL, id, ColFam.MOBILE, Event.MOBILE_DETAIL, ts, messageHash)
                            .addEventFields(basicInfo)
                            .addEventField(event, Field.CITY_ID)
                            .addEventField(event, Field.CHANNEL_ID)
                            .addEventField(event, Field.DETAIL_ID)
                            .addEventField(event, Field.DETAIL_TYPE)
                            .addEventField(event, Field.REFERER_PAGE)
                            .addEventField(event, Field.CURRENT_PAGE)
                            .addEventField(event, Field.IS_SEARCH)
                            .addEventField(event, Field.SEARCH_POSITION)
                            .addEventField(event, Field.KEYWORDS);

                    addMobileFilterItems(event, eb);

                    if (event.containsKey(Field.DETAIL_ID.value())) redisKVs.add(new Object[]{"dtl", event.get(Field.DETAIL_ID.value()),
                            DateUtil.toFormattedDate(DateUtil.parseDateTime(ts))});

                    break;

                case 2: // search
                    eb.addEvent(indexName, Event.MOBILE_SEARCH, id, ColFam.MOBILE, Event.MOBILE_SEARCH, ts, messageHash)
                            .addEventFields(basicInfo)
                            .addEventField(event, Field.CITY_ID)
                            .addEventField(event, Field.CHANNEL_ID)
                            .addEventField(event, Field.USE_SUGGESTION)
                            .addEventField(event, Field.SUGGESTION_POSITION)
                            .addEventField(event, Field.SEARCH_SOURCE)
                            .addEventField(event, Field.RESULT_NUMBER);

                    addMobileFilterItems(event, eb);

                    break;

                case 3: //follow
                    eb.addEvent(indexName, Event.MOBILE_FOLLOW, id, ColFam.MOBILE, Event.MOBILE_FOLLOW, ts, messageHash)
                            .addEventFields(basicInfo)
                            .addEventField(event, Field.CITY_ID)
                            .addEventField(event, Field.ATTENTION_ID)
                            .addEventField(event, Field.ATTENTION_ACTION)
                            .addEventField(event, Field.ATTENTION_TYPE);

                    if (event.containsKey(Field.ATTENTION_ID.value())) redisKVs.add(new Object[]{"fl", event.get(Field.ATTENTION_ID.value()),
                            DateUtil.toFormattedDate(DateUtil.parseDateTime(ts))});

                    break;

                case 6: // click
                    eb.addEvent(indexName, Event.MOBILE_OTHER_CLICK, id, ColFam.MOBILE, Event.MOBILE_OTHER_CLICK, ts, messageHash)
                            .addEventFields(basicInfo)
                            .addEventField(event, Field.CITY_ID)
                            .addEventField(event, Field.CHANNEL_ID)
                            .addEventField(event, Field.CURRENT_PAGE)
                            .addEventField(event, Field.EVENT_NAME);
                    break;

                default:
                    break;
            }
        }
    }
}
