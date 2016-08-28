package com.lianjia.profiling.tagging.counter;

import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

/**
 * buffer用法?
 * 实现有点啰嗦
 */
public class RunLengthEncoding {

    private static final int MAX_RUN_LEN = 127;
    private static final int MIN_RUN_LEN = 3;
    private static final int MAX_LITERAL_LEN = 128;
    private static final int MAX_ARR_LEN = 2048;

    public static byte[] runLenEncode(int[] input) {
        if (input.length == 0) return new byte[0];

        ByteBuffer buf = ByteBuffer.allocate((input.length + 1) * 5);

        int run = 1;
        int end = -1;
        int i;
        for (i = 1; i < input.length; i++) {
            if (input[i] == input[i - 1]) {
                run++;
                if (run >= MAX_RUN_LEN + MIN_RUN_LEN) {
                    if (i - end > run) {
                        buf.put((byte) (-(i - run - end)));
                        buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, i - run + 1)));
                    }
                    buf.put((byte) (run - MIN_RUN_LEN));
                    buf.put(varLenEncode(input[i - 1]));
                    end = i;
                    run = 0;
                }
            } else if (input[i] != input[i - 1]) {
                if (run >= MIN_RUN_LEN) {
                    if (i - end - 1 > run) {
                        buf.put((byte) (-(i - run - end - 1)));
                        buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, i - run)));
                    }
                    buf.put((byte) (run - MIN_RUN_LEN));
                    buf.put(varLenEncode(input[i - 1]));
                    end = i - 1;

                } else if (i - end - 1 >= MAX_LITERAL_LEN) {
                    buf.put((byte) (-MAX_LITERAL_LEN));
                    buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, end + MAX_LITERAL_LEN + 1)));
                    end += MAX_LITERAL_LEN;
                }

                run = 1;
            }
        }

        // tail
        if (run >= MIN_RUN_LEN) {
            if (i - end - 1 > run) {
                buf.put((byte) (-(i - run - end - 1)));
                buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, i - run)));
            }
            buf.put((byte) (run - MIN_RUN_LEN));
            buf.put(varLenEncode(input[i - 1]));
        } else if (i - end > 1) {
            while (i - end > 1) {
                if (i - end - 1 >= MAX_LITERAL_LEN) {
                    buf.put((byte) (-MAX_LITERAL_LEN));
                    buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, end + MAX_LITERAL_LEN + 1)));
                    end = end + MAX_LITERAL_LEN;
                } else {
                    buf.put((byte) (-(i - end - 1)));
                    buf.put(varLenEncode(Arrays.copyOfRange(input, end + 1, i)));
                    end = i - 1;
                }
            }
        }

        buf.flip();
        byte[] encoded = new byte[buf.remaining()];
        buf.get(encoded);

        return encoded;
    }

    public static int[] runLenDecode(byte[] data) {
        if (data.length == 0) return new int[0];

        IntBuffer intBuf = IntBuffer.allocate(MAX_ARR_LEN);

        for (int i = 0; i < data.length; ) {
            if ((data[i] & 0xFF) > 0x7F) {
                int len = -data[i];

                int num = 0;
                int pos = 0;
                int cnt = 0;
                for (i++; ; i++) {
                    num ^= (data[i] & ((1 << 7) - 1)) << 7 * pos;
                    pos += 1;

                    if ((data[i] & 0x80) == 0) {
                        intBuf.put(num);
                        if (++cnt >= len) {
                            i++;
                            break;
                        }
                        num = pos = 0;
                    }
                }

            } else {
                int num = 0;
                int len = data[i] + MIN_RUN_LEN;
                int pos = 0;
                for (i++; ; i++) {
                    num ^= (data[i] & ((1 << 7) - 1)) << 7 * pos;
                    pos += 1;

                    if ((data[i] & 0x80) == 0) {
                        i++;
                        break;
                    }
                }

                int[] arr = new int[len];
                Arrays.fill(arr, num);
                intBuf.put(arr);
            }
        }

        intBuf.flip();
        int[] ints = new int[intBuf.remaining()];
        intBuf.get(ints);

        return ints;
    }

    public static byte[] varLenEncode(int num) {
        if (num == 0) return new byte[]{0x00};

        byte[] bytes = new byte[4];
        int i = 0;
        while (true) {
            byte b = (byte) (num & 0x7F);
            num >>>= 7;
            if (num == 0) {
                bytes[i++] = b;
                break;
            }
            bytes[i++] = (byte) (b ^ 0x80);
        }

        return Arrays.copyOfRange(bytes, 0, i);
    }

    public static byte[] varLenEncode(int[] arr) {
        ByteBuffer byteBuf = ByteBuffer.allocate(arr.length * 5); // todo: 应该是多少? 是否安全?
        byteBuf.clear();
        for (int n : arr) {
            if (n == 0) {
                byteBuf.put((byte) 0);
                continue;
            }

            while (true) {
                byte b = (byte) (n & 0x7F);
                n >>>= 7;
                if (n == 0) {
                    byteBuf.put(b);
                    break;
                }
                byteBuf.put((byte) (b ^ 0x80));
            }
        }

        byteBuf.flip();
        byte[] bytes = new byte[byteBuf.remaining()];
        byteBuf.get(bytes);

        return bytes;
    }

    public static int[] varLenDecode(byte[] data) {
        IntBuffer intBuf = IntBuffer.allocate(data.length);
        intBuf.clear();
        int n = 0;
        int i = 0;
        for (byte b : data) {
            n ^= (b & ((1 << 7) - 1)) << 7 * i;
            i += 1;

            if ((b & 0x80) == 0) {
                intBuf.put(n);
                n = i = 0;
            }
        }

        intBuf.flip();
        int[] ints = new int[intBuf.remaining()];
        intBuf.get(ints);

        return ints;
    }

    public static String serializeBase64(int[] arr) {
        return new String(Base64.encodeBase64(runLenEncode(arr)));
    }

    public static int[] deserializeBase64(String base64) {
        return runLenDecode(Base64.decodeBase64(base64.getBytes()));
    }
}
