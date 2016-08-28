package com.lianjia.data.profiling.log.client;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.profiling.util.ExUtil;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.lang.management.ManagementFactory;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public abstract class BaseLogger implements Logger {
    protected String identity = ManagementFactory.getRuntimeMXBean().getName();  // pid@hostname
    protected String classNamePrefixMask = "com.lianjia.profiling.";
    protected String className;

    protected static final DateTimeFormatter DATE_FORMATTERS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSS");

    protected String format(Logger.Level level, String message) {
        return String.format("[%s %s [%d]%s %s] %s",
                             DATE_FORMATTERS.print(System.currentTimeMillis()),
                             level.name(),
                             Thread.currentThread().getId(),
                             identity,
                             className,
                             message);
    }

    @Override
    public void debug(String message) {
        System.err.println(message);
    }

    @Override
    public void debug(String format, Object ... args) {
        debug(String.format(format, args));
    }

    @Override
    public void info(String message) {
        System.err.println(message);
    }

    @Override
    public void info(String format, Object ... args) {
        info(String.format(format, args));
    }

    @Override
    public void warn(String message) {
        System.err.println(message);
    }

    @Override
    public void warn(String format, Object ... args) {
        warn(String.format(format, args));
    }

    @Override
    public void warn(String message, Throwable th) {
        warn(String.format("%s ex: %s", message, ExUtil.getStackTrace(th)));
    }

    @Override
    public void error(String message) {
        System.err.println(message);
    }

    @Override
    public void error(String format, Object ... args) {
        error(String.format(format, args));
    }

    @Override
    public void close() throws Exception {}
}
