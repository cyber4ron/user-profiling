package com.lianjia.profiling.web.controller;

import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.controller.kv.UserKVController;
import com.lianjia.profiling.web.util.RespHelper;
import com.lianjia.profiling.web.util.StatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

    @RestController
    public class AppController {
        private static final Logger LOG = LoggerFactory.getLogger(UserKVController.class.getName());

    @RequestMapping(value = "/")
    @CrossOrigin
    public ResponseEntity<String> getHomePage() {
        return new ResponseEntity<>("<h1>[Bigdata] User Profiling Homepage</h1>" +
                                            "<ul>\n" +
                                            "   <li>Wiki Home: </li>\n" +
                                            "</ul>" +
                                            "", HttpStatus.OK);
    }

    @RequestMapping("/_access/_invalidate")
    @CrossOrigin
    public ResponseEntity<String> getOfflineHouse() {
        try {
            AccessManager.buildAccounts();
            return new ResponseEntity<>(RespHelper.getSuccResp(AccessManager.getAccountAsMap()), HttpStatus.OK);
        } catch (Exception ex) {
            LOG.warn("refresh accounts failed", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "refresh accounts failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/_stat")
    @CrossOrigin
    public ResponseEntity<String> getAppStatistic() {
        try {
            return new ResponseEntity<>(RespHelper.getSuccResp(StatUtil.getStatistic()), HttpStatus.OK);
        } catch (Exception ex) {
            LOG.warn("get app statistic failed", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
