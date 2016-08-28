package com.lianjia.profiling.web.util;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class EncryptUtilTest {

    @Test
    public void testGenToken() throws Exception {
        System.out.println(EncryptUtil.genToken());
    }

    @Test
    public void testEncrypt() throws Exception {
        String token = EncryptUtil.genToken();
        System.out.println(token);
        String encrypted = EncryptUtil.encrypt(token);
        System.out.println(encrypted);
        String decrypted = EncryptUtil.decrypt("jedRPp65FAGrN7vdmSD9");
        System.out.println(decrypted);
    }
}
