package com.lianjia.profiling.tagging.counter;

import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public interface Counter<T> extends Iterable<T> {

    void add(String id, float weight);

    void decay(float rate);

    float count(String id);

    float count();

    void merge(Counter<T> Other);

    List<T> top(int k);
}
