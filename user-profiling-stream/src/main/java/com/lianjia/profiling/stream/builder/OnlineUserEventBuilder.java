package com.lianjia.profiling.stream.builder;

import com.alibaba.fastjson.JSON;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.stream.builder.OnlineUserEvents.Event;
import com.lianjia.profiling.stream.builder.OnlineUserEvents.Field;
import com.lianjia.profiling.stream.parser.OnlineUserMessageParser;
import com.lianjia.profiling.util.FieldUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class OnlineUserEventBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineUserEventBuilder.class.getName(), new HashMap<String, String>());

    public enum ColFam {
        WEB("web"),
        MOBILE("mob");

        private final String abbr;

        public String abbr() {
            return abbr;
        }

        ColFam(String abbr) {
            this.abbr = abbr;
        }
    }

    public static class Doc {
        public String idx;
        public String idxType;
        public String id;
        public Map<String, Object> doc;

        public Doc() {
            doc = new HashMap<>();
        }

        public Doc(String idx, String type, String id) {
            this.idx = idx;
            this.idxType = type;
            this.id = id;
            doc = new HashMap<>();
        }

        @Override
        public String toString() {
            return String.format("index: %s, type: %s, id: %s, doc: %s", idx, idxType, id, doc.toString());
        }
    }

    public static class EventDoc extends Doc {
        public EventDoc(String idx, String type, String id) {
            this.idx = idx;
            this.idxType = type;
            this.id = id;
        }

        @Override
        public String toString() {
            return String.format("index: %s, type: %s, id: %s, doc: %s", idx, idxType, id, doc.toString());
        }
    }

    private List<Doc> usersToES;
    private List<EventDoc> eventsToES;

    private List<Row> eventsToHbase;
    private List<Row> eventIndicesToHBase;

    private ColFam curCF;
    private Event curCQ;
    private long curTs; // timestamp of current event
    private Map<Field, Object> curEvent; // current event for hbase put
    private long curMsgHash;

    private static final Set<Field> fieldsNotToIndex = new HashSet<>();

    private int acc = 0;

    static {
        fieldsNotToIndex.add(Field.TIMESTAMP);
        fieldsNotToIndex.add(Field.OS);
        fieldsNotToIndex.add(Field.LOCATION);
    }

    /**
     * hbase data schema:
     * +-------------------+----------------------------------------------+
     * |    key format     |                  value format                |
     * +-------------------+----------------------------------------------+
     * |   userId-ts(sec)  |   eventCategory(web/mob):eventName:eventDoc  |
     * +-------------------+----------------------------------------------+
     * <p/>
     * hbase index schema (index查询模式: 查询某时间区间内, 某时间字段值为限定值的用户列表, 如按session_id筛选; 按搜索keywords筛选):
     * +---------------------------------------------------------------+----------------------+
     * |                             key format                        |     value format     |
     * +---------------------------------------------------------------+----------------------+
     * |  encodedFieldName-fieldValue-encodedEventName-roundToMin(ts)  |  key of data schema  |
     * +---------------------------------------------------------------+----------------------+
     */
    private OnlineUserEventBuilder() {
        usersToES = new ArrayList<>();
        eventsToES = new ArrayList<>();
        eventsToHbase = new ArrayList<>();
        eventIndicesToHBase = new ArrayList<>();
    }

    public static OnlineUserEventBuilder onNewMessage() {
        return new OnlineUserEventBuilder();
    }

    // add user
    public OnlineUserEventBuilder addUser(String idx, String type, String id) {
        usersToES.add(new Doc(idx, type, DigestUtils.md5Hex(id).substring(0, 16)));
        return this;
    }

    /*-------- add events --------*/

    /**
     * 维度: 消息, 事件在消息中的序数, 时间戳, 事件, 事件字段, 字段值 -> 用户
     */
    public void indexEvent() {
        for (Map.Entry<Field, Object> fieldKV : curEvent.entrySet()) {
            if (fieldsNotToIndex.contains(fieldKV.getKey())) continue;

            byte[] rowKey = eventsToHbase.get(eventsToHbase.size() - 1).getRow();
            String encodeFieldName = fieldKV.getKey().idx();
            String fieldValue = fieldKV.getKey().idx();
            String encodedEventName = curCQ.idx();
            eventIndicesToHBase.add(new Put(Bytes.toBytes(String.format("%s-%s-%s-%d",
                                                                        encodeFieldName,
                                                                        fieldValue,
                                                                        encodedEventName,
                                                                        FieldUtil.trimToMinute(curTs))))
                                            .addColumn(Bytes.toBytes(curCF.abbr()), new byte[0],
                                                       curMsgHash & 0xFFFF << 44 // 区分消息
                                                               + curTs & 0xFFFF << 28 // 区分时间戳
                                                               + new String(rowKey).hashCode() & 0xFFFF << 12 // 区分用户
                                                               + acc & 0xFFF, // 区分事件在消息中的序数
                                                       rowKey));
        }
    }

    public void addEventToHBase() {
        ((Put) eventsToHbase.get(eventsToHbase.size() - 1)).addColumn(Bytes.toBytes(curCF.abbr()),
                                                                      Bytes.toBytes(curCQ.abbr()),
                                                                      curTs * 1000000 +
                                                                              Math.abs(curMsgHash) % 1000 * 1000 + // 区分消息
                                                                              acc % 1000, // 区分事件在消息中的序数
                                                                      Bytes.toBytes(JSON.toJSONString(curEvent)));
        indexEvent();
    }

    /**
     * 维度: 消息, 事件在消息中的序数, 用户, 时间戳, 事件 -> 事件doc. 消息幂等
     *
     * @param userId ucid或uuid, 看那个不为空, 优先ucid
     * @param ts     milli sec
     */
    public OnlineUserEventBuilder addEvent(String idx, Event type, String userId, ColFam cf, Event cq, long ts, long messageHash) {
        ts = Math.max(0, ts);
        String dim = messageHash + acc + userId + ts + type.abbr();
        EventDoc event = new EventDoc(idx, type.abbr(), DigestUtils.md5Hex(dim).substring(0, 16)); // 取digest避免id过长
        eventsToES.add(event);

        if (eventsToHbase.size() > 0) addEventToHBase();

        // prepare for next event
        eventsToHbase.add(new Put(Bytes.toBytes(userId + "-" + Long.toString(ts / 1000))));
        curCF = cf;
        curCQ = cq;
        curTs = ts;
        curMsgHash = messageHash;
        curEvent = new HashMap<>();

        acc++;

        return this;
    }

    /*-------- add user field --------*/

    public OnlineUserEventBuilder addUserField(Field key, Object val) {
        if (usersToES.size() > 0 && val != null) {
            if (key.equals(Field.UCID) && !OnlineUserMessageParser.isValid(val.toString())) return this;
            if (key.equals(Field.UUID) && !OnlineUserMessageParser.isValid(val.toString())) return this;
            usersToES.get(usersToES.size() - 1).doc.put(key.abbr(), val);
        }
        return this;
    }

    /*-------- add event field --------*/

    public OnlineUserEventBuilder addEventField(Field key, Object val) {
        if (eventsToES.size() == 0 || val == null) return this;

        if (key.equals(Field.UCID) && !OnlineUserMessageParser.isValid(val.toString())) return this;
        if (key.equals(Field.UUID) && !OnlineUserMessageParser.isValid(val.toString())) return this;

        if (key.equals(Field.TIMESTAMP)) {
            long ts = Long.parseLong(val.toString());
            if (ts <= 0) return this;
            if (ts >= 1E9 && ts <= 1E10) val = ts * 1000;
        }

        if (key.equals(Field.PRICE_MIN) || key.equals(Field.PRICE_MAX)) {
            if (val.toString().equals("*")) return this;
        }

        if (key.equals(Field.HOUSE_AREA_MAX) || key.equals(Field.HOUSE_AREA_MIN)) {
            if (val.toString().equals("*")) return this;
        }

        if (key.equals(Field.IS_SEARCH)) {
            if (val.toString().toLowerCase().equals("true")) val = 1;
            else if (val.toString().toLowerCase().equals("false")) val = 0;
        }

        if (key.equals(Field.LOCATION) && val.toString().isEmpty()) return this; // todo ?
        if (key.equals(Field.OS) && val.toString().isEmpty()) return this;// todo ?

        eventsToES.get(eventsToES.size() - 1).doc.put(key.abbr(), val);
        curEvent.put(key, val);

        return this;
    }

    public OnlineUserEventBuilder addEventField(Map<String, Object> js, Field field) {
        if (js.containsKey(field.value()) && eventsToES.size() > 0) {
            addEventField(field, js.get(field.value()));
        }
        return this;
    }

    public OnlineUserEventBuilder addEventFields(Map<Field, Object> fields) {
        for (Map.Entry<Field, Object> field : fields.entrySet()) {
            addEventField(field.getKey(), field.getValue());
        }
        return this;
    }

    /*-------- getters --------*/

    public List<Doc> getUserDoc() {
        return usersToES;
    }

    public List<EventDoc> getEventDocs() {
        return eventsToES;
    }

    public List<Row> getEventIndexDocs() {
        return eventIndicesToHBase;
    }

    public List<Row> getEventsToHbase() {
        if (eventsToHbase.size() > 0) addEventToHBase();
        return eventsToHbase;
    }
}
