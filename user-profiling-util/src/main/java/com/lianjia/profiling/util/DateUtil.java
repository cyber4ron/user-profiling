package com.lianjia.profiling.util;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

import static com.lianjia.profiling.util.FieldUtil.isFieldValid;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */
public class DateUtil {
    private static final Logger LOG = Logger.getLogger(DateUtil.class);

    private static final List<DateTimeFormatter> DATE_TIME_FORMATTERS = new ArrayList<>();

    private static final List<DateTimeFormatter> DATE_FORMATTERS = new ArrayList<>();

    public static final DateTimeZone DEFAULT_TIMEZONE = DateTimeZone.getDefault(); // DateTimeZone.forOffsetHours(8);

    static {
        DATE_TIME_FORMATTERS.add(DateTimeFormat.forPattern("yyyyMMddHHmmss"));
        DATE_TIME_FORMATTERS.add(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        DATE_TIME_FORMATTERS.add(DateTimeFormat.forPattern("yyyyMMdd'T'HHmmssZ"));
        DATE_FORMATTERS.add(DateTimeFormat.forPattern("yyyyMMdd"));
        DATE_FORMATTERS.add(DateTimeFormat.forPattern("yyyy-MM-dd"));
    }

    private static final DateTimeFormatter DATE_TIME_FMT = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmssZ");
    private static final DateTimeFormatter DATE_TIME_API_FMT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private static final DateTimeFormatter DATE_FMT = DateTimeFormat.forPattern("yyyyMMdd");
    private static final DateTimeFormatter MONTH_FMT = DateTimeFormat.forPattern("yyyyMM");

    public static DateTime parseDate(String date) {
        if (!isFieldValid(date)) return null;
        DateTime dt;
        for (DateTimeFormatter fmt : DATE_FORMATTERS) {
            try {
                if ((dt = parseDateTime(fmt, date)) != null) return dt;
            } catch (Exception ignored) {
                ;
            }
        }

        return null;
    }

    public static DateTime parseDateTime(DateTimeFormatter fmt, String str) {
        if (!isFieldValid(str)) return null;
        DateTime dt;
        dt = fmt.parseDateTime(str);
        return dt;
    }

    public static DateTime parseDateTime(String date) {
        if (!isFieldValid(date)) return null;
        DateTime dt;
        for (DateTimeFormatter fmt : DATE_TIME_FORMATTERS) {
            try {
                if ((dt = parseDateTime(fmt, date)) != null) return dt;
            } catch (Exception ignored) {
                ;
            }
        }

        return null;
    }

    public static boolean isValidDate(String date) {
        return parseDate(date) != null;
    }

    public static String getDate() {
        return getDate(DATE_FMT);
    }

    public static String getYesterday() {
        return DATE_FMT.print(new DateTime().minusDays(1));
    }

    public static String getOneDayBefore() {
        return DATE_FMT.print(new DateTime().minusDays(2));
    }

    public static String getOneDayBefore(DateTime date) {
        return DATE_FMT.print(date.minusDays(1));
    }

    public static String getDate(DateTimeFormatter df) {
        return df.print(new DateTime());
    }

    public static String getMonth(String date) {
        DateTime dt = parseDate(date);
        if (dt == null) throw new IllegalArgumentException("invalid date format, date:" + date);
        return MONTH_FMT.print(dt);
    }

    public static String getMonth() {
        return getDate(MONTH_FMT);
    }

    public static Date getNext3Min() {
        DateTime dt = new DateTime();
        return dt.plusMinutes(3).toDate();
    }

    /**
     * return java.util.Date instead of joda DateTime, 避免被shade
     */
    public static Date getNextMonth() {
        DateTime dt = new DateTime();
        return dt.plusMonths(1).toDate();
    }

    public static String toDate(String str) {
        if (!isFieldValid(str)) return null;
        DateTime dt = parseDate(str);
        return dt != null ? DATE_FMT.print(dt) : null;
    }

    public static String toDateTime(Long ts) {
        if (ts < 1E12 || ts > 1E13) return null;
        DateTime dt = new DateTime(ts);
        return DATE_TIME_FMT.print(dt);
    }

    public static String toApiDateTime(Long ts) {
        if (ts < 1E12 || ts > 1E13) return null;
        DateTime dt = new DateTime(ts);
        return DATE_TIME_API_FMT.print(dt);
    }

    public static String toDateTime(String str) {
        if (!isFieldValid(str) || str.equals("0000-00-00 00:00:00")) return null;
        DateTime dt = parseDateTime(str);
        if (dt == null) dt = parseDate(str);
        if (dt == null) {
            LOG.warn("invalid datetime format: " + str);
            return null;
        } else {
            return DATE_TIME_FMT.print(dt);
        }
    }

    public static String toFormattedDate(DateTime dt) {
        return DATE_FMT.print(dt);
    }

    public static Long datetimeToTimestampMs(String str) {
        if (!isFieldValid(str)) return null;
        DateTime dt = parseDateTime(str);
        return dt != null ? dt.getMillis() : null;
    }

    public static Long dateToTimestampMs(String str) {
        if (!isFieldValid(str)) return null;
        DateTime dt = parseDate(str);
        return dt != null ? dt.getMillis() : null;
    }

    public static String dateDiff(String str, int num) {
        if (!isFieldValid(str)) return null;
        DateTime dt = parseDate(str);
        return dt != null ? DATE_FMT.print(dt.plusDays(num)) : null;
    }

    public static String alignedByWeek() {
        DateTime dt = new DateTime();
        dt = dt.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay();
        return DATE_FMT.print(dt);
    }

    public static String alignedByWeek(String date) {
        DateTime dt = DATE_FMT.parseDateTime(date);
        dt = dt.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay();
        return DATE_FMT.print(dt);
    }

    public static Date alignedByMonth() {
        DateTime dt = new DateTime();
        dt = dt.withDayOfMonth(1).withTimeAtStartOfDay();
        return dt.toDate();
    }

    public static Date alignedByMonth(Date date) {
        DateTime dt = new DateTime(date);
        dt = dt.withDayOfMonth(1).withTimeAtStartOfDay();
        return dt.toDate();
    }

    public static List<String> alignedByWeek(String start, String end) {
        Set<String> days = new HashSet<>();

        DateTime dtStart = parseDate(start);
        DateTime dtEnd = parseDate(end);
        if (dtStart == null || dtEnd == null)
            throw new IllegalArgumentException("invalid date format, start:" + start + ", end: " + end);

        for (; dtStart.compareTo(dtEnd) <= 0; dtStart = dtStart.plusDays(1)) {
            days.add(DATE_FMT.print(dtStart.withDayOfWeek(DateTimeConstants.MONDAY)));
        }

        return new ArrayList<>(days);
    }

    public static List<String> alignedByMonth(String start, String end) {
        Set<String> months = new HashSet<>();

        DateTime dtStart = parseDateTime(start);
        DateTime dtEnd = parseDateTime(end);
        if (dtStart == null || dtEnd == null)
            throw new IllegalArgumentException("invalid date format, start:" + start + ", end: " + end);

        for (; dtStart.compareTo(dtEnd) <= 0; dtStart = dtStart.plusDays(1)) {
            months.add(MONTH_FMT.print(dtStart.withDayOfMonth(1)));
        }

        return new ArrayList<>(months);
    }
}
