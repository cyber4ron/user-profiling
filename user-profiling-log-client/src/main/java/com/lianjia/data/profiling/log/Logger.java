package com.lianjia.data.profiling.log;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public interface Logger extends AutoCloseable {
    enum Level {
        TRACE(),
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    }

    void debug(String message);
    void debug(String format, Object ... args);

    void info(String message);
    void info(String format, Object ... args);

    void warn(String message);
    void warn(String format, Object ... args);
    void warn(String message, Throwable th);

    void error(String message);
    void error(String format, Object ... args);
}
