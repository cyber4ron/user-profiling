package com.lianjia.profiling.util;

import org.apache.log4j.Logger;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class FieldUtil {
    private static final Logger LOG = Logger.getLogger(FieldUtil.class);

    public static boolean isFieldValid(String str) {
        return !(str == null || str.equals("") || str.equalsIgnoreCase("null") || str.equalsIgnoreCase("\\n"));
    }

    public static Integer parseInt(String str) {
        if (!isFieldValid(str)) return null;
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    public static Long parseLong(String str) {
        if (!isFieldValid(str)) return null;
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    public static Long parseLongUnsafe(String str) {
        if (!isFieldValid(str)) throw new NumberFormatException();
        return Long.parseLong(str);
    }

    public static Double parseDouble(String str) {
        if (!isFieldValid(str)) return null;
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    public static String parsePhone(String str) {
        if (str.isEmpty() || str.equals("00000000001") || str.equalsIgnoreCase("null")) return null;
        return str.trim();
    }

    public static Long parseTsSec(String timestamp) { // e.g. 1460193481
        Long ts = parseLong(timestamp);
        if (ts == null || ts < 1E9 || ts > 1E10) return null;
        return ts;
    }

    public static Long parseTsMs(String timestamp) { // e.g. 1460193481181
        Long ts = parseLong(timestamp);
        if (ts == null) return null;
        if (ts >= 1E9 && ts <= 1E10) return ts * 1000;
        if (ts < 1E12 || ts > 1E13) return null;
        return ts;
    }

    public static Long parseTsMsUnsafe(String timestamp) { // e.g. 1460193481181
        Long ts = parseLongUnsafe(timestamp);
        if (ts == null) throw new NumberFormatException();
        if (ts >= 1E9 && ts <= 1E10) return ts * 1000;
        if (ts < 1E12 || ts > 1E13) throw new NumberFormatException();
        return ts;
    }

    //
    public static String parseUserId(Object val) {
        if (val instanceof String) {
            if (!val.equals("null")) return (String) val;
        }
        if (val instanceof Long) {
            return Long.toString((long) val);
        }
        return null;
    }

    public static String reverse(String str) {
        if (!isFieldValid(str)) return "";
        char[] seq = str.toCharArray();
        int len = seq.length;
        for (int i = 0; i < len / 2; i++) {
            char tmp = seq[i];
            seq[i] = seq[len - 1 - i];
            seq[len - 1 - i] = tmp;
        }

        return new String(seq);
    }

    public static long trimToMinute(long ts) {
        if (ts > 1E10) ts /= 1000;
        return ts - ts % 60;
    }

}
