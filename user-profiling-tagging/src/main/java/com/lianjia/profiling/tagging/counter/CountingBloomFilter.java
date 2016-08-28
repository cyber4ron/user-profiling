package com.lianjia.profiling.tagging.counter;

import com.lianjia.profiling.config.Constants;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

abstract class HashFunc {
    public abstract int hash(byte[] data, int length, int seed);

    public int hash(byte[] bytes, int init) {
        return hash(bytes, bytes.length, init);
    }
}

public class CountingBloomFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_LEN = 5000;

    private static class Hash implements Serializable {
        private static final long serialVersionUID = 1L;
        private HashFunc func;
        private int maxValue;
        private int hashNum;

        public Hash(int maxValue, int hashNum) {
            this.maxValue = maxValue;
            func = MurmurHash.getInstance();
            this.hashNum = hashNum;
        }

        public int[] hash(byte[] b) {
            if (b == null || b.length == 0) {
                throw new IllegalArgumentException("b = null or b.length = 0");
            }

            int[] result = new int[hashNum];
            for (int i = 0, init = 0; i < hashNum; i++) {
                init = func.hash(b, init);
                result[i] = Math.abs(init % maxValue);
            }

            return result;
        }
    }

    private double count = 0;

    private int[] counters;

    private Hash hash;

    public CountingBloomFilter(int n, double p) {
        counters = new int[Math.min(MAX_LEN, estimateLen(n, p))];
        int k = Math.max(1, (int) (1.0 * counters.length / n * Math.log(2)));
        hash = new Hash(counters.length, k);
    }

    public double getCount() {
        return count;
    }

    // todo rm
    public int[] getCounters() {
        return counters;
    }

    public CountingBloomFilter(String base64) {
        int[] pack = RunLengthEncoding.deserializeBase64(base64);
        int k = pack[0];
        counters = Arrays.copyOfRange(pack, 1, pack.length);
        hash = new Hash(counters.length, k);
    }

    public int estimateLen(int n, double p) {
        return Math.max(1, (int) Math.ceil(-n * Math.log(p) / Math.pow(Math.log(2), 2)));
    }

    public int setBit(int pos, int weight) {
        counters[pos] += weight;
        return counters[pos];
    }

    public int getBit(int pos) {
        return counters[pos];
    }

    public int add(byte[] element) {
        return add(element, 1);
    }

    public int add(byte[] element, double weight) {
        count += weight;
        int min = Integer.MAX_VALUE;
        for (int position : hash.hash(element)) {
            min = Math.min(setBit(position, (int) (weight / Constants.COUNTING_PRECISION + 0.5)), min);
        }

        return min;
    }

    public int count(byte[] element) {
        int[] hashes = hash.hash(element);
        int min = getBit(hashes[0]);
        for (int i = 1; i < hashes.length; i++) {
            min = Math.min(min, getBit(hashes[i]));
        }

        return min;
    }

    public CountingBloomFilter merge(CountingBloomFilter other) {
        if (other.counters.length > counters.length) {
            int[] tmp = counters;
            counters = other.counters;
            other.counters = tmp;
        }

        for (int i = 0; i < other.counters.length; i++) {
            counters[i] += other.counters[i];
        }

        return this;
    }

    public int cardinality() {
        return (int) Math.ceil(cardinalityDbl());
    }

    public double cardinalityDbl() {
        int nonZeros = 0;
        for (int i : counters) {
            if (i != 0) nonZeros += 1;
        }

        int m = counters.length;
        int k = hash.hashNum;

        return -1.0 * m * Math.log(1 - 1.0 * nonZeros / m) / k;
    }

    public void decay(double rate) {
        for(int i=0;i<counters.length;i++) {
            counters[i] *= rate;
        }
    }

    public String serializeBase64() {
        int[] pack = new int[counters.length + 1];
        pack[0] = hash.hashNum;
        System.arraycopy(counters, 0, pack, 1, counters.length);
        return RunLengthEncoding.serializeBase64(pack);
    }
}
