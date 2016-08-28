package com.lianjia.profiling.util;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class SMSUtilTest {
    @Test
    public void testMain() throws Exception {
        SMSUtil.sendMail("XX", "YY");
    }

    @Test
    public void testSendMail() throws Exception {
        for(int i=0;i<10;i++) {
            SMSUtil.sendMail("kibana start", "kibana start");
        }
    }

    @Test
    public void testSendMessage() throws Exception {
        SMSUtil.sendMessage("test");
    }
}
