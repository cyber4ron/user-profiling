package com.lianjia.profiling.tagging.features;

import com.alibaba.fastjson.JSON;
import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class Features {
    private static final Logger LOG = LoggerFactory.getLogger(Features.class.getName());

    private static final Map<Integer, PriceRangeLv1> PRICE_RANGE_LV1_IDX_MAP = new HashMap<>();
    private static final Map<Integer, PriceRangeLv2> PRICE_RANGE_LV2_IDX_MAP = new HashMap<>();
    private static final Map<Integer, PriceRangeLv3> PRICE_RANGE_LV3_IDX_MAP = new HashMap<>();
    private static final Map<Integer, AreaRange> AREA_RANGE_IDX_MAP = new HashMap<>();
    private static final Map<Integer, RoomNum> ROOM_NUM_IDX_MAP = new HashMap<>();
    private static final Map<Integer, RelativeFloorLevel> RELATIVE_FLOOR_LEVEL_IDX_MAP = new HashMap<>();
    private static final Map<Integer, BuildingAge> BUILDING_AGE_IDX_MAP = new HashMap<>();
    private static final Map<Integer, Orientation> ORIENTATION_IDX_MAP = new HashMap<>();

    public static final Map<EventType, Double> WEIGHTS = new HashMap<>();

    public static final Map<String, Class<?>> PRICE_MAPPING = new HashMap<>();

    private static final double ALPHA = 0.0666667; // 半衰期15天
    public static final int MAX_DECAY_DAYS = 10 * 365;
    public static final Map<Integer, Double> DECAY = new HashMap<>();

    private static final double PRICE_MIN = 10 * 10000;
    private static final double PRICE_MAX = 50000 * 10000;

    private static final double AREA_MIN = 10;
    private static final double AREA_MAX = 2000;

    private static final double ROOM_NUM_MIN = 1;
    private static final double ROOM_NUM_MAX = 10;

    private static final double FLOOR_TOTAL_MAX = 500;
    private static final int METRO_DIST_THD = 1200;

    private static final String WEIGHT_CONFIG = "/weights_user.json";

    private static final String INF_SIGN = "∞";

    static {
        PRICE_MAPPING.put("110000", PriceRangeLv1.class); // 北京
        PRICE_MAPPING.put("310000", PriceRangeLv1.class); // 上海
        PRICE_MAPPING.put("320500", PriceRangeLv1.class); // 苏州
        PRICE_MAPPING.put("440300", PriceRangeLv1.class); // 深圳

        PRICE_MAPPING.put("120000", PriceRangeLv2.class); // 天津
        PRICE_MAPPING.put("320100", PriceRangeLv2.class); // 南京
        PRICE_MAPPING.put("330100", PriceRangeLv2.class); // 杭州
        PRICE_MAPPING.put("370200", PriceRangeLv2.class); // 青岛
        PRICE_MAPPING.put("440100", PriceRangeLv2.class); // 广州
        PRICE_MAPPING.put("440600", PriceRangeLv2.class); // 佛山

        PRICE_MAPPING.put("210200", PriceRangeLv3.class); // 大连
        PRICE_MAPPING.put("350200", PriceRangeLv3.class); // 厦门
        PRICE_MAPPING.put("370101", PriceRangeLv3.class); // 济南
        PRICE_MAPPING.put("430100", PriceRangeLv3.class); // 长沙
        PRICE_MAPPING.put("500000", PriceRangeLv3.class); // 重庆
        PRICE_MAPPING.put("510100", PriceRangeLv3.class); // 成都

        InputStream input = Features.class.getResourceAsStream(WEIGHT_CONFIG);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        try {
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                sb.append(buffer, 0, read);
            }

            Map<String, Object> json = JSON.parseObject(sb.toString());

            for (Features.EventType group : Features.EventType.values()) {
                WEIGHTS.put(group, Double.parseDouble(json.get(group.val()).toString()));
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        for (int i = 0; i < MAX_DECAY_DAYS; i++) {
            DECAY.put(-i, 1.0 / (1 + ALPHA * i));
        }
    }


    public enum PriceRangeLv1 { // 参考北京
        LT_100       (0, 0, 100 * 10000),
        F_100_T_150  (1, 100 * 10000, 150 * 10000),
        F_150_T_200  (2, 150 * 10000, 200 * 10000),
        F_200_T_250  (3, 200 * 10000, 250 * 10000),
        F_250_T_300  (4, 250 * 10000, 300 * 10000),
        F_300_T_500  (5, 300 * 10000, 500 * 10000),
        F_500_T_800  (6, 500 * 10000, 800 * 10000),
        F_800_T_1000 (7, 800 * 10000, 1000 * 10000),
        GT_1000      (8, 1000 * 10000, Integer.MAX_VALUE);

        int idx;
        double from ,to;

        PriceRangeLv1(int idx, double from, double to) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            PRICE_RANGE_LV1_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        @SuppressWarnings("Duplicates")
        public static int idx(double price) {
            if (price < LT_100.to()) return LT_100.idx();
            else if (price >= F_100_T_150.from() && price < F_100_T_150.to()) return F_100_T_150.idx();
            else if (price >= F_150_T_200.from() && price < F_150_T_200.to()) return F_150_T_200.idx();
            else if (price >= F_200_T_250.from() && price < F_200_T_250.to()) return F_200_T_250.idx();
            else if (price >= F_250_T_300.from() && price < F_250_T_300.to()) return F_250_T_300.idx();
            else if (price >= F_300_T_500.from() && price < F_300_T_500.to()) return F_300_T_500.idx();
            else if (price >= F_500_T_800.from() && price < F_500_T_800.to()) return F_500_T_800.idx();
            else if (price >= F_800_T_1000.from() && price < F_800_T_1000.to()) return F_800_T_1000.idx();
            else if (price >= GT_1000.from()) return GT_1000.idx();
            else return -1;
        }

        @SuppressWarnings("Duplicates")
        public static int[] indices(double min, double max) {
            List<Integer> indices = new ArrayList<>();
            if (!(max < LT_100.from || min > LT_100.to)) indices.add(LT_100.idx());
            if (!(max < F_100_T_150.from || min > F_100_T_150.to)) indices.add(F_100_T_150.idx());
            if (!(max < F_150_T_200.from || min > F_150_T_200.to)) indices.add(F_150_T_200.idx());
            if (!(max < F_200_T_250.from || min > F_200_T_250.to)) indices.add(F_200_T_250.idx());
            if (!(max < F_250_T_300.from || min > F_250_T_300.to)) indices.add(F_250_T_300.idx());
            if (!(max < F_300_T_500.from || min > F_300_T_500.to)) indices.add(F_300_T_500.idx());
            if (!(max < F_500_T_800.from || min > F_500_T_800.to)) indices.add(F_500_T_800.idx());
            if (!(max < F_800_T_1000.from || min > F_800_T_1000.to)) indices.add(F_800_T_1000.idx());
            if (!(max < GT_1000.from || min > GT_1000.to)) indices.add(GT_1000.idx());

            int[] covered = new int[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                covered[i] = indices.get(i);
            }

            return covered;
        }

        public static String repr(int idx) {
            if(!PRICE_RANGE_LV1_IDX_MAP.containsKey(idx)) return null;
            return String.format("[%dw, %sw)", (int) (PRICE_RANGE_LV1_IDX_MAP.get(idx).from() / 10000),
                                 idx != GT_1000.idx() ? String.valueOf(PRICE_RANGE_LV1_IDX_MAP.get(idx).to() / 10000) : INF_SIGN);
        }
    }

    public enum PriceRangeLv2 { // 参考天津
        LT_80       (0, 0, 80 * 10000),
        F_80_T_100  (1, 80 * 10000, 100 * 10000),
        F_100_T_120 (2, 100 * 10000, 120 * 10000),
        F_120_T_150 (3, 120 * 10000, 150 * 10000),
        F_150_T_200 (4, 150 * 10000, 200 * 10000),
        F_200_T_300 (5, 200 * 10000, 300 * 10000),
        F_300_T_500 (6, 300 * 10000, 500 * 10000),
        GT_500      (7, 500 * 10000, Integer.MAX_VALUE);

        int idx;
        double from ,to;

        PriceRangeLv2(int idx, double from, double to) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            PRICE_RANGE_LV2_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        @SuppressWarnings("Duplicates")
        public static int idx(double price) {
            if (price < LT_80.to()) return LT_80.idx();
            else if (price >= F_80_T_100.from() && price < F_80_T_100.to()) return F_80_T_100.idx();
            else if (price >= F_100_T_120.from() && price < F_100_T_120.to()) return F_100_T_120.idx();
            else if (price >= F_120_T_150.from() && price < F_120_T_150.to()) return F_120_T_150.idx();
            else if (price >= F_150_T_200.from() && price < F_150_T_200.to()) return F_150_T_200.idx();
            else if (price >= F_200_T_300.from() && price < F_200_T_300.to()) return F_200_T_300.idx();
            else if (price >= F_300_T_500.from() && price < F_300_T_500.to()) return F_300_T_500.idx();
            else if (price >= GT_500.from()) return GT_500.idx();
            else return -1;
        }

        @SuppressWarnings("Duplicates")
        public static int[] indices(double min, double max) {
            List<Integer> indices = new ArrayList<>();
            if (!(max < LT_80.from || min > LT_80.to)) indices.add(LT_80.idx());
            if (!(max < F_80_T_100.from || min > F_80_T_100.to)) indices.add(F_80_T_100.idx());
            if (!(max < F_100_T_120.from || min > F_100_T_120.to)) indices.add(F_100_T_120.idx());
            if (!(max < F_120_T_150.from || min > F_120_T_150.to)) indices.add(F_120_T_150.idx());
            if (!(max < F_150_T_200.from || min > F_150_T_200.to)) indices.add(F_150_T_200.idx());
            if (!(max < F_200_T_300.from || min > F_200_T_300.to)) indices.add(F_200_T_300.idx());
            if (!(max < F_300_T_500.from || min > F_300_T_500.to)) indices.add(F_300_T_500.idx());
            if (!(max < GT_500.from || min > GT_500.to)) indices.add(GT_500.idx());

            int[] covered = new int[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                covered[i] = indices.get(i);;
            }

            return covered;
        }

        public static String repr(int idx) {
            if(!PRICE_RANGE_LV2_IDX_MAP.containsKey(idx)) return null;
            return String.format("[%dw, %sw)", (int) (PRICE_RANGE_LV2_IDX_MAP.get(idx).from() / 10000),
                                 idx != GT_500.idx() ? String.valueOf(PRICE_RANGE_LV2_IDX_MAP.get(idx).to() / 10000) : INF_SIGN);
        }
    }

    public enum PriceRangeLv3 { // 参考重庆
        LT_30       (0, 0, 30 * 10000),
        F_30_T_50   (1, 30 * 10000, 50 * 10000),
        F_50_T_80   (2, 50 * 10000, 80 * 10000),
        F_80_T_100  (3, 80 * 10000, 100 * 10000),
        F_100_T_150 (4, 100 * 10000, 150 * 10000),
        F_150_T_200 (5, 150 * 10000, 200 * 10000),
        F_200_T_300 (6, 200 * 10000, 300 * 10000),
        GT_300      (7, 300 * 10000, Integer.MAX_VALUE);

        int idx;
        double from ,to;

        PriceRangeLv3(int idx, double from, double to) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            PRICE_RANGE_LV3_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        @SuppressWarnings("Duplicates")
        public static int idx(double price) {
            if (price < LT_30.to()) return LT_30.idx();
            else if (price >= F_50_T_80.from() && price < F_50_T_80.to()) return F_50_T_80.idx();
            else if (price >= F_80_T_100.from() && price < F_80_T_100.to()) return F_80_T_100.idx();
            else if (price >= F_100_T_150.from() && price < F_100_T_150.to()) return F_100_T_150.idx();
            else if (price >= F_150_T_200.from() && price < F_150_T_200.to()) return F_150_T_200.idx();
            else if (price >= F_200_T_300.from() && price < F_200_T_300.to()) return F_200_T_300.idx();
            else if (price >= GT_300.from()) return GT_300.idx();
            else return -1;
        }

        @SuppressWarnings("Duplicates")
        public static int[] indices(double min, double max) {
            List<Integer> indices = new ArrayList<>();
            if (!(max < LT_30.from || min > LT_30.to)) indices.add(LT_30.idx());
            if (!(max < F_80_T_100.from || min > F_80_T_100.to)) indices.add(F_80_T_100.idx());
            if (!(max < F_50_T_80.from || min > F_50_T_80.to)) indices.add(F_50_T_80.idx());
            if (!(max < F_100_T_150.from || min > F_100_T_150.to)) indices.add(F_100_T_150.idx());
            if (!(max < F_150_T_200.from || min > F_150_T_200.to)) indices.add(F_150_T_200.idx());
            if (!(max < F_200_T_300.from || min > F_200_T_300.to)) indices.add(F_200_T_300.idx());
            if (!(max < GT_300.from || min > GT_300.to)) indices.add(GT_300.idx());

            int[] covered = new int[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                covered[i] = indices.get(i);;
            }

            return covered;
        }

        public static String repr(int idx) {
            if(!PRICE_RANGE_LV3_IDX_MAP.containsKey(idx)) return null;
            return String.format("[%dw, %sw)", (int) (PRICE_RANGE_LV3_IDX_MAP.get(idx).from() / 10000),
                                 idx != GT_300.idx() ? String.valueOf(PRICE_RANGE_LV3_IDX_MAP.get(idx).to() / 10000) : INF_SIGN);
        }
    }

    public enum AreaRange {
        LT_50       (0, 0, 50),
        F_50_T_70   (1, 50, 70),
        F_70_T_90   (2, 70, 90),
        F_90_T_110  (3, 90, 110),
        F_110_T_130 (4, 110, 130),
        F_130_T_150 (5, 130, 150),
        F_150_T_200 (6, 150, 200),
        GT_200      (7, 200, Integer.MAX_VALUE);

        int idx;
        double from ,to;

        AreaRange(int idx, double from, double to) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            AREA_RANGE_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        @SuppressWarnings("Duplicates")
        public static int idx(double area) {
            if (area < LT_50.to()) return LT_50.idx();
            else if (area >= F_50_T_70.from() && area < F_50_T_70.to()) return F_50_T_70.idx();
            else if (area >= F_70_T_90.from() && area < F_70_T_90.to()) return F_70_T_90.idx();
            else if (area >= F_90_T_110.from() && area < F_90_T_110.to()) return F_90_T_110.idx();
            else if (area >= F_110_T_130.from() && area < F_110_T_130.to()) return F_110_T_130.idx();
            else if (area >= F_130_T_150.from() && area < F_130_T_150.to()) return F_130_T_150.idx();
            else if (area >= F_150_T_200.from() && area < F_150_T_200.to()) return F_150_T_200.idx();
            else if (area >= GT_200.from()) return GT_200.idx();

            else return -1;
        }

        @SuppressWarnings("Duplicates")
        public static int[] indices(double min, double max) {
            List<Integer> indices = new ArrayList<>();
            if (!(max < LT_50.from() || min > LT_50.to())) indices.add(LT_50.idx());
            if (!(max < F_50_T_70.from() || min > F_50_T_70.to())) indices.add(F_50_T_70.idx());
            if (!(max < F_70_T_90.from() || min > F_70_T_90.to())) indices.add(F_70_T_90.idx());
            if (!(max < F_90_T_110.from() || min > F_90_T_110.to())) indices.add(F_90_T_110.idx());
            if (!(max < F_150_T_200.from() || min > F_150_T_200.to())) indices.add(F_150_T_200.idx());
            if (!(max < F_110_T_130.from() || min > F_110_T_130.to())) indices.add(F_110_T_130.idx());
            if (!(max < F_130_T_150.from() || min > F_130_T_150.to())) indices.add(F_130_T_150.idx());
            if (!(max < GT_200.from() || min > GT_200.to())) indices.add(GT_200.idx());

            int[] covered = new int[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                covered[i] = indices.get(i);
            }

            return covered;
        }

        public static String repr(int idx) {
            if(!AREA_RANGE_IDX_MAP.containsKey(idx)) return null;
            return String.format("[%fm², %fm²)", new BigDecimal(AREA_RANGE_IDX_MAP.get(idx).from).setScale(3, RoundingMode.HALF_UP).doubleValue(),
                                 new BigDecimal(AREA_RANGE_IDX_MAP.get(idx).to).setScale(3, RoundingMode.HALF_UP).doubleValue());
        }
    }

    public enum RoomNum {
        ONE     (0),
        TWO     (1),
        THREE   (2),
        FOUR    (3),
        FIVE    (4),
        GT_FIVE (5);

        int idx;

        public int idx() {
            return idx;
        }

        RoomNum(int idx) {
            this.idx = idx;
            ROOM_NUM_IDX_MAP.put(idx, this);
        }
        @SuppressWarnings("Duplicates")
        public static int idx(int num) {
            if (num == ONE.idx()) return ONE.idx();
            if (num == TWO.idx()) return TWO.idx();
            if (num == THREE.idx()) return THREE.idx();
            if (num == FOUR.idx()) return FOUR.idx();
            if (num == FIVE.idx()) return FIVE.idx();
            if (num >= GT_FIVE.idx()) return GT_FIVE.idx();

            return -1;
        }

        @SuppressWarnings("Duplicates")
        public static int[] indices(int min, int max) {
            List<Integer> indices = new ArrayList<>();
            if (min == ONE.idx() || max == ONE.idx()) indices.add(ONE.idx());
            if (min == TWO.idx() || max == TWO.idx()) indices.add(TWO.idx());
            if (min == THREE.idx() || max == THREE.idx()) indices.add(THREE.idx());
            if (min == FOUR.idx() || max == FOUR.idx()) indices.add(FOUR.idx());
            if (min == FIVE.idx() || max == FIVE.idx()) indices.add(FIVE.idx());
            if (min >= GT_FIVE.idx() || max >= GT_FIVE.idx()) indices.add(GT_FIVE.idx());

            int[] covered = new int[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                covered[i] = indices.get(i);;
            }

            return covered;
        }

        public static String repr(int idx) {
            if(!ROOM_NUM_IDX_MAP.containsKey(idx)) return null;
            return String.valueOf(ROOM_NUM_IDX_MAP.get(idx).idx);
        }
    }

    public enum RelativeFloorLevel { // 如6层, 1/2 - low, 3/4 - mid, 5/6 -> high, todo: 改整形?
        LOW (0, 0, 1.0 / 3 + 1E-6, "低层"),
        MID (1, 1.0 / 3 + 1E-6, 2.0 / 3 + 1E-6, "中层"),
        HIGH(2, 2.0 / 3 + 1E-6, 1, "高层");

        int idx;
        double from ,to;
        String val;

        RelativeFloorLevel(int idx, double from , double to, String val) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            this.val = val;
            RELATIVE_FLOOR_LEVEL_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public double from() {
            return from;
        }

        public double to() {
            return to;
        }

        public String val() {return val;}

        public static int indices(int floor, int total) {
            double lv = (double) floor / total;
            if(lv > LOW.from() && lv <= LOW.to()) return LOW.idx();
            if(lv > MID.from() && lv <= MID.to()) return MID.idx();
            if(lv > HIGH.from()) return HIGH.idx();

            return -1;
        }

        public static String repr(int idx) {
            if(!RELATIVE_FLOOR_LEVEL_IDX_MAP.containsKey(idx)) return null;
            return RELATIVE_FLOOR_LEVEL_IDX_MAP.get(idx).val();
        }
    }

    public enum BuildingAge { // 年限较长的对于偏好没什么意义
        LT_5        (0, 0, 5),
        F_5_T_10    (1, 5, 10),
        F_10_T_20   (2, 10, 20),
        G_T_20      (3, 20, Integer.MAX_VALUE);

        int idx, from ,to;

        BuildingAge(int idx, int from, int to) {
            this.idx = idx;
            this.from = from;
            this.to = to;
            BUILDING_AGE_IDX_MAP.put(idx, this);
        }

        public int idx() {
            return idx;
        }

        public int from() {
            return from;
        }

        public int to() {
            return to;
        }

        public static int idx(int diff) {
            if(diff > LT_5.from() && diff <= LT_5.to()) return LT_5.idx();
            if(diff > F_5_T_10.from() && diff <= F_5_T_10.to()) return F_5_T_10.idx();
            if(diff > F_10_T_20.from() && diff <= F_10_T_20.to()) return F_10_T_20.idx();
            if(diff > G_T_20.from()) return G_T_20.idx();
            return -1;
        }

        public static String repr(int idx) {
            if(!BUILDING_AGE_IDX_MAP.containsKey(idx)) return null;
            return String.format("[%d年, %s年)", BUILDING_AGE_IDX_MAP.get(idx).from(),
                                 idx != G_T_20.idx() ? BUILDING_AGE_IDX_MAP.get(idx).to(): INF_SIGN);
        }
    }

    public enum Orientation {
        EAST        (0, 200300000001L, "东"),
        SOUTH       (1, 200300000003L, "南"),
        WEST        (2, 200300000005L, "西"),
        NORTH       (3, 200300000007L, "北"),
        NORTH_SOUTH (4, 200300000002L, "东南"),
        WEST_SOUTH  (5, 200300000004L, "西南"),
        WEST_NORTH  (6, 200300000006L, "西北"),
        EAST_NORTH  (7, 200300000008L, "东北");

        int idx;
        long code;
        String val;

        Orientation(int idx, long code, String val) {
            this.idx = idx;
            this.code = code;
            this.val = val;
            ORIENTATION_IDX_MAP.put(idx, this);
        }

        public long idx() {
            return idx;
        }

        public long code() {
            return code;
        }

        public String val() {
            return val;
        }

        public static int idx(long code) {
            if(code == EAST .code) return EAST .idx;
            if(code == SOUTH.code) return SOUTH.idx;
            if(code == WEST .code) return WEST .idx;
            if(code == NORTH.code) return NORTH.idx;
            if(code == NORTH_SOUTH.code) return NORTH_SOUTH.idx;
            if(code == WEST_SOUTH .code) return WEST_SOUTH .idx;
            if(code == WEST_NORTH .code) return WEST_NORTH .idx;
            if(code == EAST_NORTH .code) return EAST_NORTH .idx;
            return -1;
        }

        public static String repr(int idx) {
            if(!ORIENTATION_IDX_MAP.containsKey(idx)) return null;
            return ORIENTATION_IDX_MAP.get(idx).val();
        }
    }

    public enum BizType {
        SELL(0),
        RENT(1);

        int idx;

        BizType(int idx) {
            this.idx = idx;
        }

        public int idx() {
            return idx;
        }
    }

    public enum EventType {
        DELEGATION("delegation"),
        TOURING("touring"),
        CONTRACT("contract"),
        PC_DETAIL("pc_detail"),
        PC_SEARCH("pc_search"),
        PC_FOLLOW("pc_follow"),
        MOBILE_DETAIL("mobile_detail"),
        MOBILE_SEARCH("mobile_search"),
        MOBILE_FOLLOW("mobile_follow"),
        MOBILE_CLICK("mobile_click");

        String val;

        EventType(String val) {
            this.val = val;
        }

        public String val() {
            return this.val;
        }
    }


    public static boolean isPriceValid(double price) {
        return PRICE_MIN <= price && price < PRICE_MAX;
    }

    public static boolean isAreaValid(double area) {
        return AREA_MIN <= area && area < AREA_MAX;
    }

    public static boolean isRoomNumValid(double roomNum) {
        return ROOM_NUM_MIN <= roomNum && roomNum < ROOM_NUM_MAX;
    }

    public static boolean isFloorValid(int floor, int total) {
        return floor > 0 && total > 0 && floor <= total && total < FLOOR_TOTAL_MAX;
    }

    public static boolean isBoolValueValid(String val) {
        return val.equals("0") || val.equals("1");
    }

    public static int getPriceIdx(String cityId, double price) {
        if(!PRICE_MAPPING.containsKey(cityId) || !isPriceValid(price)) return -1;
        Class<?> range = PRICE_MAPPING.get(cityId);
        if (PriceRangeLv1.class.isAssignableFrom(range)) return PriceRangeLv1.idx(price);
        else if (PriceRangeLv2.class.isAssignableFrom(range)) return PriceRangeLv2.idx(price);
        else if (PriceRangeLv3.class.isAssignableFrom(range)) return PriceRangeLv3.idx(price);
        else return -1;
    }

    public static int[] getPriceIdx(String cityId, double priceMin, double priceMax) {
        if(!PRICE_MAPPING.containsKey(cityId) || !isPriceValid(priceMin) || !isPriceValid(priceMax)) return new int[0];
        Class<?> range = PRICE_MAPPING.get(cityId);
        if (PriceRangeLv1.class.isAssignableFrom(range)) return PriceRangeLv1.indices(priceMin, priceMax);
        else if (PriceRangeLv2.class.isAssignableFrom(range)) return PriceRangeLv2.indices(priceMin, priceMax);
        else if (PriceRangeLv3.class.isAssignableFrom(range)) return PriceRangeLv3.indices(priceMin, priceMax);
        else return new int[0];
    }

    public static int getAreaIdx(double area) {
        return isAreaValid(area) ? AreaRange.idx(area) : -1;
    }

    public static int[] getAreaIdx(double areaMin, double areaMax) {
        if (!isAreaValid(areaMin) || !isAreaValid(areaMax)) return new int[0];
        return AreaRange.indices(areaMin, areaMax);
    }

    public static int[] getRoomNumIdx(int roomMin, int roomMax) {
        if(!isRoomNumValid(roomMin) || !isRoomNumValid(roomMax)) return new int[0];
        return RoomNum.indices(roomMin, roomMax);
    }

    public static int getRoomNumIdx(int roomNum) {
        if(!isRoomNumValid(roomNum)) return -1;
        return RoomNum.idx(roomNum);
    }

    public static int getRelativeFloorLevel(int floor, int total) {
        if(!isFloorValid(floor, total)) return -1;
        return RelativeFloorLevel.indices(floor, total);
    }

    public static int getOrientIdx(long orientCode) {
        return Orientation.idx(orientCode);
    }

    public static int getBuildingAgeIdx(int eventYear, int year) {
        if(year < 1500 ||year > 3000) return -1;
        return BuildingAge.idx(eventYear - year);
    }

    public static int isMetro(String description) {
        StringBuilder sb = new StringBuilder();
        char[] seq = description.toCharArray();
        for (int i = 0; i < seq.length; i++) {
            if (seq[i] >= '0' && seq[i] <= '9' && (i == 0 || seq[i - 1] >= '0' && seq[i - 1] <= '9'))
                sb.append(seq[i]);
        }

        try {
            return Integer.parseInt(sb.toString()) <= METRO_DIST_THD ? 1 : 0;
        } catch (Exception ex) {
            return -1;
        }
    }

    public static int getBizType(String type) {
        if (type.equals("200200000001")) return BizType.SELL.idx();
        if (type.equals("200200000002")) return BizType.RENT.idx();
        return -1;
    }

    public static int getDateDiff(Map<String, Object> event, String[] keys) {
        return getDateDiff(event, keys, System.currentTimeMillis());
    }

    public static int getDate(Map<String, Object> event, String[] keys) {
        for(String key : keys) {
            if(event.containsKey(key)) {
                return DateUtil.parseDateTime(event.get(key).toString()).getYear();
            }
        }

        throw new IllegalArgumentException("event does not contains key: " + StringUtils.join(keys));
    }

    public static int getDateDiff(Map<String, Object> event, String[] keys, long ts) {
        for(String key : keys) {
            if(event.containsKey(key)) {
                int dateDiff = Days.daysBetween(new DateTime(ts),
                                                DateUtil.parseDateTime(event.get(key).toString())).getDays();

                if (dateDiff > 0 || dateDiff <= -MAX_DECAY_DAYS) {
                    LOG.warn("invalid date: " + event.get(key).toString());
                    return  -1;
                }

                return dateDiff;
            }
        }

        return -1;
    }
}
