package com.lianjia.profiling.stream;

import static org.apache.commons.lang3.StringEscapeUtils.unescapeJava;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class MessageUtil {
    public static String removeQuotes(String line) {
        return line.replace("\"events\":\"[", "\"events\":[")
                .replace("}]\"", "}]")
                .replace("\"filter_items\":\"{", "\"filter_items\":{")
                .replace("}\"", "}");
    }

    public static String[] extract(String line) {
        // extract server timestamp
        String serverTs = "";
        int idx = line.indexOf("INFO:");
        if(idx != -1) {
            serverTs = line.substring(idx + 6, idx + 25);
        }

        int ipStart = line.indexOf("ip[");
        int ipEnd = line.indexOf("token[");
        String ip = line.substring(ipStart + 3, ipEnd - 2);

        int start = line.indexOf("[bigdata.");
        if (start == -1) return new String[]{};

        String extracted = line.substring(start, line.length());
        String[] parts = extracted.split("\t");

        if (parts.length < 3) return new String[]{};
        String msgType = parts[0];
        String msg = parts[2];

        return new String[]{ msgType, msg, serverTs, ip };
    }

    public static String unescape(String line) {
        String tmp = line;
        String unescaped = unescapeJava(line);
        while (!unescaped.equals(tmp)) {
            tmp = unescaped;
            unescaped = unescapeJava(unescaped);
        }
        return unescaped;
    }
}
