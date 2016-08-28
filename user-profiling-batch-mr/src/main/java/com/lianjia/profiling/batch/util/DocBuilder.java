package com.lianjia.profiling.batch.util;

import org.apache.hadoop.io.*;

import static com.lianjia.profiling.util.FieldUtil.*;
import static com.lianjia.profiling.util.DateUtil.*;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class DocBuilder {
    private MapWritable doc;

    public DocBuilder() {
        doc = new MapWritable();
    }

    public static DocBuilder newDoc() {
        return new DocBuilder();
    }

    public MapWritable getDoc() {
        return doc;
    }

    public DocBuilder addText(String key, String val) {
        if (isFieldValid(val)) doc.put(new Text(key), new Text(val));
        return this;
    }

    public DocBuilder addInt(String key, String val) {
        Integer num;
        if ((num = parseInt(val)) != null) doc.put(new Text(key), new IntWritable(num));
        return this;
    }

    public DocBuilder addLong(String key, String val) {
        Long num;
        if ((num = parseLong(val)) != null) doc.put(new Text(key), new LongWritable(num));
        return this;
    }

    public DocBuilder addDouble(String key, String val) {
        Double num;
        if ((num = parseDouble(val)) != null) doc.put(new Text(key), new DoubleWritable(num));
        return this;
    }

    public DocBuilder addDate(String key, String val) {
        String date;
        if ((date = toDate(val)) != null) doc.put(new Text(key), new Text(date));
        return this;
    }
}
