package com.lianjia.profiling.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class Properties {
    private static java.util.Properties props = new java.util.Properties();

    private Properties() {}

    static {
        try (InputStream input = Properties.class.getResourceAsStream("/config.properties")) {
            props.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    public static String getOrDefault(String key, String defaultVal) {
        if(props.containsKey(key)) return props.getProperty(key);
        else return defaultVal;
    }
}
