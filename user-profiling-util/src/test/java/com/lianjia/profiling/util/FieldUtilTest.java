package com.lianjia.profiling.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.Arrays;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class FieldUtilTest {

    @Test
    public void testReverse() throws Exception {
        System.out.println(FieldUtil.reverse(""));
        System.out.println(FieldUtil.reverse("abc"));
        System.out.println(FieldUtil.reverse("ab"));
        System.out.println(FieldUtil.reverse("中文"));
        System.out.println(FieldUtil.reverse("中文123"));
    }

    @Test
    public void testEncode() throws Exception {
        // byte[] decoded = BaseEncoding.base16().decode("ABCD");

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update("4eed6b73-ab78-4b7c-b3a8-819c7ad77621-pv-1462460054934-0".getBytes());

        byte[] x = md5.digest();
        System.out.println(Arrays.toString(x));

        System.out.println(x);
        System.out.println(new String(x));

        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        messageDigest.update("4eed6b73-ab78-4b7c-b3a8-819c7ad77621-pv-1462460054934-0".getBytes());
        String encryptedString = new String(messageDigest.digest());
        System.out.println(encryptedString);

        String hex = DigestUtils.md5Hex("4eed6b73-ab78-4b7c-b3a8-819c7ad77621-pv-1462460054934-0");
        System.out.println(hex);
        System.out.println(hex.length());
        hex = DigestUtils.md5Hex("4eed6b73-ab79-4b7c-b3a8-819c7ad77621-pv-1462460054934-0");
        System.out.println(hex);
        System.out.println(hex.length());
        String sha1 = DigestUtils.sha1Hex("4eed6b73-ab78-4b7c-b3a8-819c7ad77621-pv-1462460054934-0");
        System.out.println(sha1);
        System.out.println(sha1.length());
    }
}
