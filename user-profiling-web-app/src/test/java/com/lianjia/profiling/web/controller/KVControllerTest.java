package com.lianjia.profiling.web.controller;

import com.lianjia.profiling.web.controller.kv.UserKVController;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class KVControllerTest {

    UserKVController ctlr;

    @Before
    public void setUp() throws Exception {
    ctlr = new UserKVController();
}

    @After
    public void tearDown() throws Exception {

    }

    //@Test
    public void testGetCustomerOnline() throws Exception {
//        String s = ctlr.getUserOnline("15801378848", "", "", "0", "200", "false");
//        System.out.println(s);
    }

    //@Test
    public void testGetCustomerOffline() throws Exception {
            ctlr.getUserOffline("15801378848", "jeRPp5FAGN7vdSD9", "true");
    }

    @Test
    public void testRewriteField() throws Exception {
        Map<String ,Object> xx = new HashMap<>();
        xx.put("x", 24);
        xx.put("ch_id", 3);
        Map<String ,Object> x = ctlr.rewriteFields(xx);
        System.out.println(x);
    }
}
