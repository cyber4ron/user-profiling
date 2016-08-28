package com.lianjia.profiling.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class ExUtil {
    public static String getStackTrace(Throwable ex) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        ex.printStackTrace(printWriter);
        printWriter.flush();
        return writer.toString();
    }
}
