package com.lianjia.profiling.tagging.counter;

import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public class HashCounterTest {

    @Test
    public void test() throws Exception {
        HashCounter counter1 = new HashCounter();
        counter1.add("001", 0.5F);
        counter1.add("002", 0.5F);
        counter1.add("003", 0.5F);
        counter1.add("001", 0.5F);

        List<Map.Entry<String, Float>> top = counter1.top(2);

        for (Map.Entry<String, Float> e: top) {
            System.out.println(e.getKey() + ", " + e.getValue());
        }
        System.out.println();


        HashCounter counter2 = new HashCounter();
        counter2.add("001", 0.5F);
        counter2.add("002", 0.5F);
        counter2.add("003", 0.5F);
        counter2.add("004", 0.5F);

        HashCounter counter3 = HashCounter.merge(counter1, counter2);
        for(Map.Entry<String, Float> e: counter3.top(4)) {
            System.out.println(e.getKey() + ", " + e.getValue());
        }

        System.out.println(counter1.count());

        counter1.merge(counter2);
        counter1.decay(0.8F);

        for(Map.Entry<String, Float> e: counter1.top(5)) {
            System.out.println(e.getKey() + ", " + e.getValue());
        }

        System.out.println(counter1.count("001"));
        System.out.println(counter1.count());
    }
}
