package com.lianjia.profiling.web.util;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class EncryptUtil {
    private static final int[] SALT = new int[]{2, 5, 9, 13};
    private static final int[] SALT_INV = new int[]{13, 9, 5, 2};
    private static final Random RAND = new Random();
    private static final int LEN = 16;

    private static final char[] symbols;

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        for (char ch = 'A'; ch <= 'Z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    /**
     * @return 16 digit alpha-numeric string
     */
    public static String genToken() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, LEN).forEach(x -> sb.append(symbols[RAND.nextInt(62)]));
        return sb.toString();
    }

    public static String encrypt(String token) {
        StringBuilder sb = new StringBuilder(token);
        IntStream.of(SALT_INV).boxed().collect(Collectors.toList()).forEach(pos -> sb.insert(pos.intValue(), symbols[RAND.nextInt(62)]));
        return sb.toString();
    }

    public static String decrypt(String encrypted) {
        StringBuilder sb = new StringBuilder(encrypted);
        IntStream.of(SALT).boxed().forEach(sb::deleteCharAt);
        return sb.toString();
    }
}
