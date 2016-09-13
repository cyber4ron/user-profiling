package com.lianjia.profiling.tagging.features;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.config.Constants;
import com.lianjia.profiling.tagging.counter.HashCounter;
import com.lianjia.profiling.tagging.features.Features.*;
import com.lianjia.profiling.tagging.tag.UserTag;
import com.lianjia.profiling.util.DateUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class UserPreference implements Serializable {

    private static final long serialVersionUID = 2L;

    private static final Map<String, UserTag> FIELD_MAP = new HashMap<>();
    private static final Set<UserTag> CATEGORY_FIELDS = new HashSet<>();
    private static final Set<UserTag> CATEGORY_CNT_FIELDS = new HashSet<>();
    private static final Set<UserTag> ID_FIELDS = new HashSet<>();
    private static final Set<UserTag> ID_CNT_FIELDS = new HashSet<>();
    private static final HashMap<UserTag, UserTag> CNT_FIELD_MAP = new HashMap<>();
    private static final Set<UserTag> BOOL_FIELDS = new HashSet<>();
    private static final Set<UserTag> BOOL_CNT_FIELDS = new HashSet<>();
    private static final double MAX_BF_FALSE_POSITIVE = 0.05;
    private static final String RANGE_COUNTER_SUFFIX = "cnt";
    private static final String ID_COUNTER_SUFFIX = "cbf";
    private static final long DEFAULT_TS = System.currentTimeMillis() - 86400L * 1000 * 3000;

    static {
        CATEGORY_FIELDS.add(UserTag.ROOM_NUM);
        CATEGORY_FIELDS.add(UserTag.AREA);
        CATEGORY_FIELDS.add(UserTag.PRICE_LV1);
        CATEGORY_FIELDS.add(UserTag.PRICE_LV2);
        CATEGORY_FIELDS.add(UserTag.PRICE_LV3);
        CATEGORY_FIELDS.add(UserTag.FLOOR);
        CATEGORY_FIELDS.add(UserTag.ORIENT);
        CATEGORY_FIELDS.add(UserTag.BUILDING_AGE);

        CATEGORY_CNT_FIELDS.add(UserTag.ROOM_NUM_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.AREA_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.PRICE_LV1_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.PRICE_LV2_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.PRICE_LV3_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.FLOOR_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.ORIENT_CNT);
        CATEGORY_CNT_FIELDS.add(UserTag.BUILDING_AGE_CNT);

        ID_FIELDS.add(UserTag.DISTRICT);
        ID_FIELDS.add(UserTag.BIZCIRCLE);
        ID_FIELDS.add(UserTag.RESBLOCK);

        ID_CNT_FIELDS.add(UserTag.DISTRICT_CNT);
        ID_CNT_FIELDS.add(UserTag.BIZCIRCLE_CNT);
        ID_CNT_FIELDS.add(UserTag.RESBLOCK_CNT);

        BOOL_FIELDS.add(UserTag.UNIQUE);
        BOOL_FIELDS.add(UserTag.SCHOOL);
        BOOL_FIELDS.add(UserTag.METRO);

        BOOL_CNT_FIELDS.add(UserTag.UNIQUE_CNT);
        BOOL_CNT_FIELDS.add(UserTag.SCHOOL_CNT);
        BOOL_CNT_FIELDS.add(UserTag.METRO_CNT);

        CNT_FIELD_MAP.put(UserTag.ROOM_NUM_CNT, UserTag.ROOM_NUM);
        CNT_FIELD_MAP.put(UserTag.AREA_CNT, UserTag.AREA);
        CNT_FIELD_MAP.put(UserTag.PRICE_LV1_CNT, UserTag.PRICE_LV1);
        CNT_FIELD_MAP.put(UserTag.PRICE_LV2_CNT, UserTag.PRICE_LV2);
        CNT_FIELD_MAP.put(UserTag.PRICE_LV3_CNT, UserTag.PRICE_LV3);
        CNT_FIELD_MAP.put(UserTag.FLOOR_CNT, UserTag.FLOOR);
        CNT_FIELD_MAP.put(UserTag.ORIENT_CNT, UserTag.ORIENT);
        CNT_FIELD_MAP.put(UserTag.BUILDING_AGE_CNT, UserTag.BUILDING_AGE);

        CNT_FIELD_MAP.put(UserTag.DISTRICT_CNT, UserTag.DISTRICT);
        CNT_FIELD_MAP.put(UserTag.BIZCIRCLE_CNT, UserTag.BIZCIRCLE);
        CNT_FIELD_MAP.put(UserTag.RESBLOCK_CNT, UserTag.RESBLOCK);

        CNT_FIELD_MAP.put(UserTag.UNIQUE_CNT, UserTag.UNIQUE);
        CNT_FIELD_MAP.put(UserTag.SCHOOL_CNT, UserTag.SCHOOL);
        CNT_FIELD_MAP.put(UserTag.METRO_CNT, UserTag.METRO);

        for (UserTag tag : UserTag.values()) {
            FIELD_MAP.put(tag.val(), tag);
        }
    }

    // houseId + src + type -> [cnt, lastTs]
    private Map<String, Object[]> historyHousedIds;
    private final static long HISTORY_HOUSED_IDS_TTL = 1000 * 60 * 60 * 24 * 180L;

    private Map<UserTag, Object> entries;

    public UserPreference() {
        entries = new HashMap<>();
        historyHousedIds = new HashMap<>();
        entries.put(UserTag.WRITE_TS, DEFAULT_TS);
    }

    private static int total(int[] counter) {
        int sum = 0;
        for (int i : counter) sum += i;
        return sum;
    }

    public static void sort(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            boolean done = true;
            for (int j = 1; j < arr.length - i; j++) {
                if (arr[j - 1] < arr[j]) {
                    int tmp = arr[j - 1];
                    arr[j - 1] = arr[j];
                    arr[j] = tmp;
                    done = false;
                }
            }
            if (done) break;
        }
    }

    private static <T> void sort(double[] arr, T[] par) {
        for (int i = 0; i < arr.length - 1; i++) {
            boolean done = true;
            for (int j = 1; j < arr.length - i; j++) {
                if (arr[j - 1] < arr[j]) {
                    double tmp = arr[j - 1];
                    arr[j - 1] = arr[j];
                    arr[j] = tmp;

                    T obj = par[j - 1];
                    par[j - 1] = par[j];
                    par[j] = obj;

                    done = false;
                }
            }
            if (done) break;
        }
    }

    private int[] merge(int[] base, int[] inc) {
        if (base.length != inc.length) {
            return base;
        }

        for (int i = 0; i < inc.length; i++) {
            base[i] += inc[i];
        }

        return base;
    }

    private static <T> T[] dedup(T[] arr) {
        if (arr.length == 0) return arr;
        Set<T> set = new HashSet<>();
        int next = 0;
        for (int i = 0; i < arr.length; i++) {
            if (!set.contains(arr[i])) {
                set.add(arr[i]);
                T tmp = arr[next];
                arr[next] = arr[i];
                arr[i] = tmp;
                next++;
            }
        }

        return Arrays.copyOfRange(arr, 0, next);
    }

    public static <T> String join(T[][] arr, String sep) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            sb.append("(");
            sb.append(String.valueOf(arr[i][0]));
            sb.append(",");
            sb.append(String.valueOf(arr[i][1]));
            sb.append(")");
            if(i != arr.length - 1) sb.append(sep);
        }
        sb.append("]");
        return sb.toString();
    }

    public Object[] concat(Object[] a, Object[] b) {
        Object[] concat = new Object[a.length + b.length];
        System.arraycopy(a, 0, concat, 0, a.length);
        System.arraycopy(b, 0, concat, a.length, b.length);

        return concat;
    }

    public static Object[][] cloneArray(Object[][] src) {
        int length = src.length;
        if(length == 0 || src[0].length == 0) return new Object[0][0];
        Object[][] target = new Object[length][src[0].length];
        for (int i = 0; i < length; i++) {
            System.arraycopy(src[i], 0, target[i], 0, src[i].length);
        }
        return target;
    }

    public void updateMeta(UserTag key, Object val) {
        entries.put(key, val);
    }

    public void updateCategoryField(UserTag field, int idx, double weight) {
        int[] counter;
        if (entries.containsKey(field.counter())) {
            counter = (int[]) entries.get(field.counter());
        } else {
            counter = new int[field.size()];
        }

        counter[(int) idx] += (int) (weight / Constants.COUNTING_PRECISION + 0.5);

        computeCategoryField(field, counter);
    }

    public void updateBoolField(UserTag field, Object val, double weight) {
        if (!String.valueOf(val).equals("0") && !String.valueOf(val).equals("1")) {
            return;
        }

        updateCategoryField(field, String.valueOf(val).equals("1") ? 1 : 0, weight);
    }

    public void updateIdField(UserTag field, String id, float weight) {

        Object[][] objs = entries.containsKey(field) ? cloneArray((Object[][]) entries.get(field)) : new Object[0][0];

        String[] values = new String[objs.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = (String) objs[i][0];
        }

        HashCounter counter;
        if (entries.containsKey(field.counter())) {
            counter = (HashCounter) entries.get(field.counter());
        } else {
            counter = new HashCounter();
        }

        counter.add(id, weight);

        String[] candidates = dedup(ArrayUtils.addAll(values, id));

        computeIdField(field, candidates, counter);
    }

    public void update(UserTag field, Object val, float weight) {
        if (CATEGORY_FIELDS.contains(field)) {
            updateCategoryField(field, (int) val, weight);
        } else if (BOOL_FIELDS.contains(field)) {
            updateBoolField(field, val, weight);
        } else if (ID_FIELDS.contains(field)) {
            updateIdField(field, (String) val, weight);
        } else {
            throw new IllegalStateException("config error.");
        }
    }

    public void computeCategoryField(UserTag field, int[] counter) {

        int num = field.topNum();
        double support = field.minSupport();
        double percent = field.percent();
        Integer[] top = new Integer[num];

        int total = total(counter);
        int acc = 0;

        double[] copy = new double[counter.length];
        for (int i = 0; i < counter.length; i++) {
            copy[i] = counter[i];
        }
        Integer[] range = new Integer[copy.length];
        for (int i = 0; i < range.length; i++) {
            range[i] = i;
        }

        sort(copy, range);
        double[] percents = new double[num];
        for (int i = 0; i < copy.length; i++) {
            if (acc >= num || copy[i] == 0) break;
            if (/*copy[i] * Constants.COUNTING_PRECISION < support ||*/ copy[i] / total < percent) break;

            top[acc] = range[i];
            percents[acc] = 1.0 * copy[i] / total;
            acc++;
        }

        if (acc > 0) {
            Object[][] updated = new Object[Math.min(num, acc)][2];
            for (int i = 0; i < acc; i++) {
                updated[i][0] = top[i];
                updated[i][1] = percents[i];
            }
            entries.put(field, updated);
        } else entries.remove(field);

        entries.put(field.counter(), counter);
    }

    /**
     * @param candidates primitive values
     */
    public void computeIdField(UserTag field, Object[] candidates, HashCounter counter) {
        double[] counts = new double[candidates.length];

        for (int i = 0; i < candidates.length; i++) {
            counts[i] = counter.count(candidates[i].toString()) * Constants.COUNTING_PRECISION;
        }

        sort(counts, candidates);

        int num = field.topNum();
        double support = field.minSupport();
        double percent = field.percent();
        double count = counter.count();

        int acc = 0;
        double[] percents = new double[candidates.length];
        for (int i = 0; i < candidates.length && acc < num; i++) {
            if (/*counts[i] < support || */counts[i] / count < percent || counts[i] == 0) break;
            percents[i] = counts[i] / count;
            acc++;
        }

        if (acc > 0) {
            Object[][] updated = new Object[acc][2];
            for (int i = 0; i < acc; i++) {
                updated[i][0] = candidates[i];
                updated[i][1] = percents[i];
            }
            entries.put(field, updated);
        } else entries.remove(field);

        entries.put(field.counter(), counter);
    }

    private void mergeCategoryField(Map<UserTag, Object> otherEntries, UserTag field) {
        if (entries.containsKey(field.counter()) && otherEntries.containsKey(field.counter())) {
            int[] counter = merge((int[]) entries.get(field.counter()), (int[]) otherEntries.get(field.counter()));
            computeCategoryField(field, counter);
        } else if (otherEntries.containsKey(field.counter())) {
            if(otherEntries.containsKey(field)) entries.put(field, otherEntries.get(field));
            if(otherEntries.containsKey(field.counter())) entries.put(field.counter(), otherEntries.get(field.counter()));
        }
    }

    private Object[] project(Object[][] arr, int idx) {
        Object[] res = new Object[arr.length];
        for (int i = 0; i < arr.length; i++) {
            res[i] = arr[i][idx];
        }

        return res;
    }

    private void mergeIdField(Map<UserTag, Object> otherEntries, UserTag field) {
        if (entries.containsKey(field.counter()) && otherEntries.containsKey(field.counter())) {
            Object[] candidates = dedup(concat((entries.containsKey(field) ? project((Object[][]) entries.get(field), 0): new Object[0]),
                                          (otherEntries.containsKey(field)? project((Object[][]) otherEntries.get(field), 0): new Object[0])));

            HashCounter counter = HashCounter.merge((HashCounter) entries.get(field.counter()),
                                                    ((HashCounter) otherEntries.get(field.counter())));

            computeIdField(field, candidates, counter);
        } else if (otherEntries.containsKey(field)) {
            if(otherEntries.containsKey(field)) entries.put(field, otherEntries.get(field));
            if(otherEntries.containsKey(field.counter())) entries.put(field.counter(), otherEntries.get(field.counter()));
        }
    }

    public void merge(UserPreference other) {
        for (Map.Entry<UserTag, Object> e : other.entries.entrySet()) {
            if (CATEGORY_CNT_FIELDS.contains(e.getKey()) || BOOL_CNT_FIELDS.contains(e.getKey()))
                mergeCategoryField(other.entries, CNT_FIELD_MAP.get(e.getKey()));
            else if (ID_CNT_FIELDS.contains(e.getKey())) {
                mergeIdField(other.entries, CNT_FIELD_MAP.get(e.getKey()));
            } else if (e.getKey() == UserTag.UCID) {
                updateMeta(UserTag.UCID, e.getValue());
            } else if (e.getKey() == UserTag.UUID) {
                updateMeta(UserTag.UUID, e.getValue());
            }
        }

        // for(Map.Entry<String, Object[]> e: other.historyHousedIds.entrySet()) {
        //     updateHistoryHouseIdAlt(e.getKey(), e.getValue());
        // }
    }

    public static UserPreference decayAndMerge(UserPreference x, UserPreference y) {
        // 对齐时间尺度到当前
        x.decay();
        y.decay();

        // 这里x没有做为immutable, 提高效率
        x.merge(y);
        return x;
    }

    private void decayCategoryField(UserTag field, double regression) {
        int[] counter = entries.containsKey(field) ? (int[]) entries.get(field.counter()) : new int[0];

        for (int i = 0; i < counter.length; i++) {
            counter[i] *= regression;
        }

        computeCategoryField(field, counter);
    }

    private void decayIdField(UserTag field, float rate) {
        String[] candidates = entries.containsKey(field) ?
                Arrays.copyOf((Object[]) entries.get(field), ((Object[]) entries.get(field)).length, String[].class)
                : new String[0];

        HashCounter counter = (HashCounter) entries.get(field.counter());
        counter.decay(rate);

        computeIdField(field, candidates, counter);
    }

    @SuppressWarnings("ConstantConditions")
    public void decay() {
        int dateDiff = Days.daysBetween(new DateTime(),
                                        DateUtil.parseDateTime((long) entries.get(UserTag.WRITE_TS))).getDays();

        if (dateDiff > 0 || dateDiff <= Features.MAX_DECAY_DAYS) {
            // LOG.warn("invalid date: " + entries.get(Tag.WRITE_TS).toString());
            return;
        }

        float regression = (float) (double) Features.DECAY.get(dateDiff);

        for (Map.Entry<UserTag, Object> e : entries.entrySet()) {
            UserTag field = e.getKey();
            if (CATEGORY_FIELDS.contains(field) || BOOL_FIELDS.contains(field)) {
                decayCategoryField(field, regression);
            } else if (ID_FIELDS.contains(field)) {
                decayIdField(field, regression);
            } else {
                // log
                throw new IllegalStateException("");
            }
        }

        updateMeta(UserTag.WRITE_TS, System.currentTimeMillis());
    }

    public static boolean isCategoryField(UserTag tag) {
        return CATEGORY_FIELDS.contains(tag);
    }

    public static boolean isIdField(UserTag tag) {
        return ID_FIELDS.contains(tag);
    }

    public static boolean isBoolField(UserTag tag) {
        return BOOL_FIELDS.contains(tag);
    }

    public void updateHistoryHouseId(String houseId, String type, Long ts) {
        String key = houseId + "," + type;
        if (historyHousedIds.containsKey(key)) {
            Object[] stat = historyHousedIds.get(key);
            historyHousedIds.put(key, new Object[]{((int) stat[0]) + 1, Math.max(ts, (long) stat[1])});
        } else {
            historyHousedIds.put(key, new Object[]{1, ts});
        }
    }

    private void updateHistoryHouseIdAlt(String key, Object[] values) {
        if (historyHousedIds.containsKey(key)) {
            Object[] stat = historyHousedIds.get(key);
            historyHousedIds.put(key, new Object[]{((int) values[0]) + 1, Math.max((long) values[1], (long) stat[1])});
        } else {
            historyHousedIds.put(key, new Object[]{1, values[1]});
        }
    }

    public long getWriteTs() {
        if(entries.containsKey(UserTag.WRITE_TS)) return (long) entries.get(UserTag.WRITE_TS);
        else {
            entries.put(UserTag.WRITE_TS, System.currentTimeMillis());
            return (long) entries.get(UserTag.WRITE_TS);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<UserTag, Object> e : entries.entrySet()) {
            if (e.getKey().val().equals("phone")) {
                sb.append(String.format("%s: %s", "phone", e.getValue().toString()));
            } else if (e.getKey().val().equals("city_id")) {
                sb.append(String.format("%s: %s", "city_id", e.getValue().toString()));
            } else if (e.getKey().val().endsWith(RANGE_COUNTER_SUFFIX)) {
                sb.append(String.format("%s: %s", e.getKey(), Arrays.toString((int[]) e.getValue())));
            } else if (e.getKey().val().endsWith(ID_COUNTER_SUFFIX)) {
                StringBuilder counters = new StringBuilder(e.getKey() + ": ");
                int idx = 0;
                for (Map.Entry<String, Float> ent: (HashCounter) e.getValue()) {
                    if (ent.getValue() != 0) {
                        counters.append(String.format("(%d,%f),", idx++, ent.getValue()));
                    }
                }
                sb.append(counters.substring(0, counters.length() - 1));
            } else if (e.getKey().val().equals(UserTag.AREA.val()) ||
                    e.getKey().val().equals(UserTag.ROOM_NUM.val()) ||
                    e.getKey().val().equals(UserTag.FLOOR.val()) ||
                    e.getKey().val().equals(UserTag.ORIENT.val()) ||
                    e.getKey().val().equals(UserTag.BUILDING_AGE.val()) ||
                    e.getKey().val().equals(UserTag.DISTRICT.val()) ||
                    e.getKey().val().equals(UserTag.BIZCIRCLE.val()) ||
                    e.getKey().val().equals(UserTag.RESBLOCK.val()) ||
                    e.getKey().val().equals(UserTag.UNIQUE.val()) ||
                    e.getKey().val().equals(UserTag.SCHOOL.val()) ||
                    e.getKey().val().equals(UserTag.METRO.val()) ||
                    e.getKey().val().startsWith("price_")) {
                sb.append(String.format("%s: %s", e.getKey(), Arrays.toString((Object[]) e.getValue())));
            } else {
                sb.append(String.format("%s: %s", e.getKey().val(), e.getValue().toString()));
            }

            sb.append("\n");
        }

        return sb.toString();
    }

    public String toJson() {
        entries.put(UserTag.WRITE_TS, DateUtil.toDateTime(System.currentTimeMillis()));

        Map<String, Object> doc = new HashMap<>();
        for (Map.Entry<UserTag, Object> e : entries.entrySet()) {
            if (e.getKey().val().endsWith(RANGE_COUNTER_SUFFIX)
                    || e.getKey().val().endsWith(ID_COUNTER_SUFFIX)
                    || !FIELD_MAP.containsKey(e.getKey().val())) continue;
            doc.put(e.getKey().val(), e.getValue());
        }

        return JSON.toJSONString(doc);
    }

    @SuppressWarnings("Duplicates")
    public String toReadableJson() {
        Map<String, Object> repr = new HashMap<>();

        for (Map.Entry<UserTag, Object> e : entries.entrySet()) {
            if (e.getKey().val().endsWith(RANGE_COUNTER_SUFFIX)
                    || e.getKey().val().endsWith(ID_COUNTER_SUFFIX)
                    || !FIELD_MAP.containsKey(e.getKey().val())) continue;

            Object[][] vals;
            Object[][] tags;
            StringBuilder sb;
            switch (e.getKey().val()) {
                case "room_num":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = RoomNum.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "area":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = AreaRange.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "price_lv1":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = PriceRangeLv1.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "price_lv2":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = PriceRangeLv2.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "price_lv3":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = PriceRangeLv3.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "orient":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = Orientation.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "floor":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = RelativeFloorLevel.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;


                case "bd_age":
                    vals = (Object[][]) e.getValue();
                    tags = new Object[vals.length][2];
                    for (int i = 0; i < vals.length; i++) {
                        tags[i][0] = BuildingAge.repr(Integer.valueOf(vals[i][0].toString()));
                        tags[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    repr.put(e.getKey().repr(), tags);
                    break;

                case "district":

                case "bizcircle":

                case "resblock":
                    repr.put(e.getKey().repr(), e.getValue());
                    vals = (Object[][]) e.getValue();
                    for (int i = 0; i < vals.length; i++) {
                        vals[i][1] = new BigDecimal(Double.parseDouble(vals[i][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    }
                    break;

                case "unique":

                case "school":

                case "metro":
                    vals = (Object[][]) e.getValue();
                    if (vals.length != 1) break;
                    repr.put(e.getKey().repr(), new Object[]{vals[0][0].toString().equals("1"),
                            new BigDecimal(Double.parseDouble(vals[0][1].toString())).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()});
                    break;

                default:
                    break;
            }
        }

        // repr.put("ids", historyHousedIds);

        return JSON.toJSONString(repr);
    }

    public String getId() {
        return entries.containsKey(UserTag.UCID)? entries.get(UserTag.UCID).toString(): entries.get(UserTag.UUID).toString();
    }

    public Map<UserTag, Object> getEntries() {
        return entries;
    }

    /**
     * todo use kryo
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            return bos.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    public static UserPreference deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            UserPreference prefer = (UserPreference) in.readObject();
            for (Map.Entry<String, Object[]> e : prefer.historyHousedIds.entrySet()) {
                if (System.currentTimeMillis() - (long) (e.getValue()[1]) > HISTORY_HOUSED_IDS_TTL)
                    prefer.historyHousedIds.remove(e.getKey());
            } // ConcurrentModificationException

            return prefer;

        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}
