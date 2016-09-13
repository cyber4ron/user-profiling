package com.lianjia.profiling.tagging.counter;

import java.io.Serializable;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public class HashCounter implements Counter<Map.Entry<String, Float>>, Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Float> counter;
    private float count;

    public HashCounter() {
        counter = new HashMap<>();
        count = 0F;
    }

    @Override
    public void add(String id, float weight) {
        if (counter.containsKey(id)) counter.put(id, counter.get(id) + weight);
        else counter.put(id, weight);
        count += weight;
    }

    @Override
    public float count(String id) {
        if (counter.containsKey(id)) return counter.get(id);
        else return 0F;
    }

    @Override
    public float count() {
        return count;
    }

    @Override
    public void decay(float rate) {
        for (Map.Entry<String, Float> e : counter.entrySet()) {
            e.setValue(e.getValue() * rate);
        }

        count *= rate;
    }

    @Override
    public void merge(Counter<Map.Entry<String, Float>> other) {
        for (Map.Entry<String, Float> e : other) {
            if (counter.containsKey(e.getKey())) counter.put(e.getKey(), counter.get(e.getKey()) + e.getValue());
            else counter.put(e.getKey(), e.getValue());
        }
    }

    // O(nlogn)
    @Override
    public List<Map.Entry<String, Float>> top(int k) {
        List<Map.Entry<String, Float>> entries = new ArrayList<>(counter.entrySet());
        Collections.sort(entries, new Comparator<Object>() {
            @SuppressWarnings("unchecked")
            @Override
            public int compare(Object o1, Object o2) {
                return ((Map.Entry<String, Float>) o2).getValue() - ((Map.Entry<String, Float>) o1).getValue() > 0 ? 1 : -1;
            }
        });

        return entries.subList(0, Math.min(entries.size(), k));
    }

    public static HashCounter merge(HashCounter x, HashCounter y) {
        HashCounter hc = new HashCounter();
        hc.merge(x);
        hc.merge(y);
        return hc;
    }

    @Override
    public Iterator<Map.Entry<String, Float>> iterator() {
        return new Iterator<Map.Entry<String, Float>>() {
            private  Iterator<Map.Entry<String, Float>> it = counter.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Map.Entry<String, Float> next() {
                return it.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
