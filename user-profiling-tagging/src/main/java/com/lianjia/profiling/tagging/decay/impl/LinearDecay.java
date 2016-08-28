package com.lianjia.profiling.tagging.decay.impl;

import com.lianjia.profiling.tagging.decay.Decay;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-07
 */

public class LinearDecay implements Decay {

    public static final int MAX_DECAY_DAYS;

    static {
        DateTime epoch = new DateTime().withYear(2006)
                .withMonthOfYear(1)
                .withDayOfMonth(1)
                .withZone(DateTimeZone.getDefault());

        MAX_DECAY_DAYS = Days.daysBetween(epoch,
                                          new DateTime().withZone(DateTimeZone.getDefault())).getDays();
    }

    private double rate;

    private Map<Integer, Double> decays = new HashMap<>();

    public LinearDecay(double rate) {
        this.rate = rate;

        double decay = 1;
        for (int i = 0; i <= MAX_DECAY_DAYS; i++) {
            decays.put(i, decay);
            decay *= this.rate;
        }
    }

    @Override
    public double decay(int days) {
        if(days > MAX_DECAY_DAYS || days < 0)
            throw new IllegalArgumentException("illegal days: " + days);

        return decays.get(days);
    }
}
