package com.lianjia.profiling.monitor.ljscreen;

import com.lianjia.profiling.util.SMSUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class RoutineChecker {
    private static final Logger LOG = LoggerFactory.getLogger(RoutineChecker.class.getName());

    private Timer timer = new Timer(RoutineChecker.class.getName(), false);

    public void start() {
        TimerTask aliveTask = new TimerTask() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public void run() {
                try {
                    String statAll = LjScreenStat.getStatSum();
                    String statDetail = LjScreenStat.getStatDetail();

                        SMSUtil.sendMail("大屏幕监控(左侧统计信息)", statAll + "<br>" + statDetail,
                                         "datamining-rd@lianjia.com");

                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }
        };

         timer.scheduleAtFixedRate(aliveTask, new DateTime(DateTimeZone.forOffsetHours(8)).plusDays(1).withTimeAtStartOfDay()
                 .plusHours(9).toDate(), 24 * 60 * 60 * 1000);
    }

    public static void main(String[] args) {
        RoutineChecker checker = new RoutineChecker();
        checker.start();
    }
}
