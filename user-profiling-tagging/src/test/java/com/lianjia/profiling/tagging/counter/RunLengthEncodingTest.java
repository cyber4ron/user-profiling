package com.lianjia.profiling.tagging.counter;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class RunLengthEncodingTest {

    @Test
    public void testRunLenEncode() throws Exception {

        byte b = -128;

        int[] nums = new int[]{1, 2, 3, 4, 5};
        byte[] en = RunLengthEncoding.runLenEncode(nums);
        int[] de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        nums = new int[]{1, 2, 2, 2, 5};
        en = RunLengthEncoding.runLenEncode(nums);
        de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        nums = new int[]{1, 2, 3, 3, 3, 3, 3, 3, 5};
        en = RunLengthEncoding.runLenEncode(nums);
        de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        nums = new int[]{1};
        en = RunLengthEncoding.runLenEncode(nums);
        de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        //
        IntBuffer buf = IntBuffer.allocate(1024);
        nums = new int[200];
        Arrays.fill(nums, 288);
        buf.put(new int[]{345678, 5678, 2, 0, 0, 0, 0, 0});
        buf.put(nums);
        buf.put(new int[]{345678, 5678, 2, 0, 0, 0, 0, 0});

        buf.flip();
        nums = new int[buf.remaining()];
        buf.get(nums);

        en = RunLengthEncoding.runLenEncode(nums);
        de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        //
        buf = IntBuffer.allocate(1024);
        nums = new int[200];
        Arrays.fill(nums, 0);
        buf.put(new int[]{345678, 5678, 2, 0, 0, 0, 0, 0});
        buf.put(nums);
        buf.put(new int[]{345678, 5678, 2, 0, 0, 0, 0, 0});

        buf.flip();
        nums = new int[buf.remaining()];
        buf.get(nums);

        en = RunLengthEncoding.runLenEncode(nums);
        de = RunLengthEncoding.runLenDecode(en);
        Assert.assertArrayEquals(nums, de);

        ////
        int len = 1024;
        for (int t = 0; t < 10000; t++) {
            nums = new int[len];
            Arrays.fill(nums, 0);

            for (int i = 0; i < 1024; i++) {
                nums[ThreadLocalRandom.current().nextInt(0, nums.length)] += 1;
            }

            System.out.println("num:");
            for(int i=0;i<nums.length;i++) {
                System.out.print(nums[i] + ",");
            }
            System.out.println();

            en = RunLengthEncoding.runLenEncode(nums);
            System.out.println("en:");
            for(int i=0;i<en.length;i++) {
                System.out.print(en[i] + ",");
            }
            System.out.println();

            de = RunLengthEncoding.runLenDecode(en);
            System.out.println("de:");
            for(int i=0;i<de.length;i++) {
                System.out.print(de[i] + ",");
            }
            System.out.println();

            if (nums.length != de.length) {
                System.out.println();
                break;
            }

            for (int i = 0; i < nums.length; i++) {
                if (nums[i] != de[i]) {
                    System.out.println();
                    break;
                }
            }
        }

        System.out.println();
    }

    @Test
    public void testRunLenDecode() throws Exception {
        int[] nums = new int[]{3,5,4,2,1,2,2,2,0,0,2,1,4,3,1,2,2,3,1,2,1,2,3,1,3,1,5,3,2,2,5,4,1,4,3,3,4,1,6,5,2,6,2,3,1,6,4,3,1,1,0,4,1,3,0,1,5,1,3,2,3,2,4,2,3,3,5,2,4,4,3,4,5,7,2,0,6,0,2,2,4,3,2,2,6,1,5,5,3,3,1,4,0,4,2,4,4,3,2,6,6,2,5,2,3,1,0,4,3,4,7,4,5,5,4,2,3,1,5,3,2,3,4,1,2,3,1,2,1,6,2,5,4,5,6,3,1,1,1,2,5,6,1,0,1,2,1,1,1,6,1,4,6,2,3,3,3,2,2,2,2,1,5,1,3,3,2,5,4,5,0,1,4,3,2,5,7,3,0,1,8,3,4,3,1,4,3,2,2,3,4,3,2,6,3,2,0,4,5,5,7,1,2,3,8,5,2,3,2,2,5,4,2,3,4,2,5,4,0,3,4,2,3,3,3,2,3,3,5,1,5,2,1,8,4,2,1,1,4,2,2,2,2,3,5,4,2,4,4,2,2,3,1,0,3,3,2,2,1,4,2,8,4,6,3,4,3,3,4,3,2,2,3,3,4,3,2,2,6,3,0,0,3,1,1,4,7,4,6,6,2,2,3,3,4,3,1,3,5,3,6,3,3,3,3,4,1,6,0,6,4,2,5,4,2,3,5,1,1,3,3,1,3,3,0,3,7,3,4,3,1,1,6,3,4,4,1,3,3,3,2,3,1,0,5,3,2,2,1,5,2,3,5,0,6,3,2,2,2,1,4,5,4,4,1,2,6,1,1,4,4,1,3,3,5,4,3,7,4,3,1,3,2,2,2,4,1,5,5,1,3,4,2,2,3,2,4,3,2,5,3,6,0,0,5,1,2,5,3,3,2,4,2,2,3,2,4,1,3,1,3,2,5,2,4,2,0,2,3,3,1,0,2,3,1,3,4,1,4,2,2,4,0,3,4,3,2,4,6,5,5,4,2,3,3,2,4,5,0,1,4,1,5,6,2,4,2,4,3,0,5,4,2,3,5,1,3,3,5,2,4,4,2,2,1,1,4,2,4,0,2,3,2,4,1,2,4,4,6,3,3,1,2,1,1,0,1,5,4,8,4,2,4,2,4,5,5,3,5,1,1,4,4,2,3,4,4,2,1,3,4,1,2,6,3,1,4,1,4,6,1,3,2,2,3,0,4,2,4,2,3,1,1,4,2,3,4,1,1,3,1,2,1,4,2,5,2,3,1,5,2,3,7,3,6,7,5,2,5,5,3,4,2,0,5,2,3,0,3,4,2,0,3,2,5,2,0,1,4,4,1,2,4,6,7,3,2,4,5,4,2,3,1,0,3,5,2,4,0,7,3,2,2,3,3,3,2,6,2,3,5,6,2,6,1,4,1,1,3,4,2,3,5,3,5,6,2,1,4,1,4,0,2,4,2,1,2,1,5,2,6,1,0,5,1,1,5,3,3,2,0,3,5,3,0,1,1,3,2,2,4,2,3,3,2,4,3,2,2,3,1,2,1,3,6,3,5,4,3,2,5,7,6,2,2,6,3,2,1,2,1,2,2,2,2,3,3,2,2,3,0,1,3,3,3,4,4,3,3,3,3,5,6,2,2,1,3,5,2,0,1,2,1,7,4,1,1,4,4,2,1,4,8,3,1,4,3,2,2,5,5,3,4,5,2,5,2,4,3,1,5,4,2,3,5,4,2,3,4,3,7,5,4,6,1,2,4,3,4,1,4,4,3,3,3,2,5,5,4,3,1,2,4,4,5,1,7,4,5,4,4,3,5,1,4,6,2,0,1,2,3,3,2,1,3,1,2,1,5,0,2,3,7,0,1,0,3,1,6,2,5,3,2,5,1,1,2,4,3,4,3,5,4,0,3,1,2,1,2,1,1,3,0,3,4,3,3,2,3,3,3,6,1,1,3,1,2,3,3,2,4,1,3,2,7,4,4,1,2,6,1,5,2,2,2,4,2,2,3,5,6,1,2,1,5,3,2,1,4,3,3,4,2,5,2,4,3,4,3,4,3,3,2,4,3,3,6,4,0,4,4,3,7,5,2,4,4,8,3,4,1,5,3,3,5,3,2,1,3,7,0,2,2,5,4,5,0,4,3,2,2,1,2,5,1,5,4,1,2,4,1,2,1,3,1,1,5,1,2,3,2,3,2,2,3,1,1,2,1,1,2,4,3,1,5,2,6,2,6,3,2,0,2,4,6,1,3,4,7,3,5,4,3,4,5,2,2,8,4,1,3,1,0,0};
        System.out.println("num:");
        for(int i=0;i<nums.length;i++) {
            System.out.print(nums[i] + ",");
        }
        System.out.println();
        System.out.println();

        byte[] en = RunLengthEncoding.runLenEncode(nums);
        System.out.println("en:");
        for(int i=0;i<en.length;i++) {
            System.out.print(en[i] + ",");
        }
        System.out.println();
        System.out.println();

        int[] de = RunLengthEncoding.runLenDecode(en);
        System.out.println("de:");
        for(int i=0;i<de.length;i++) {
            System.out.print(de[i] + ",");
        }
        System.out.println();
    }

    @Test
    public void testVarLenEncode() throws Exception {

        int[] nums = new int[]{2, 300, 0x123456, 0, 1, -1, 1 << 7, 1 << 8, 1 << 10, 0x12345678, Integer.MAX_VALUE, Integer.MIN_VALUE};
        byte[] ser = RunLengthEncoding.varLenEncode(nums);
        int[] de = RunLengthEncoding.varLenDecode(ser);

        byte[] ser2 = RunLengthEncoding.varLenEncode(new int[]{2, 5, 5, 6, 6, 6});

        String encoded = new String(Base64.encodeBase64(ser2));
        byte[] decoded = Base64.decodeBase64(encoded.getBytes());

        int[] de2 = RunLengthEncoding.varLenDecode(decoded);

        System.out.println();
    }
}
