package com.lianjia.profiling.web.controller.archive;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.dao.HBaseDao;
import com.lianjia.profiling.web.util.FutureUtil;
import com.lianjia.profiling.web.util.RespHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

@RestController
public class UserArchiveController {
    private static final Logger LOG = LoggerFactory.getLogger(UserArchiveController.class.getName());
    // hbase use hbase.client.scanner.timeout.period config
    private static final int QUERY_TIMEOUT_MS = 60 * 1000;

    private HBaseDao hbaseDao;

    public UserArchiveController() {
        hbaseDao = new HBaseDao();
    }

    /**
     * @param start ms
     * @param end   ms
     */
    @RequestMapping(value = "/archive/user/online/{ucid}", method = RequestMethod.GET)
    @CrossOrigin
    public ResponseEntity<String> batchGetOfflineHouse(@PathVariable("ucid") String ucid,
                                                       @RequestParam(value = "start", required = true) String start,
                                                       @RequestParam(value = "end", required = true) String end,
                                                       @RequestParam(value = "token", required = true) String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();
            Map<String, Object> resp = new HashMap<>();

            CompletableFuture<List<?>> future = FutureUtil.getCompletableFuture(() -> null/*hbaseDao.get(ucid,
                                                                                                   FieldUtil.parseTsMsUnsafe(start),
                                                                                                   FieldUtil.parseTsMsUnsafe(end))*/);
            List<?> res = future.get(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            resp.put("data", res);
            resp.put("num", res.size());

            return new ResponseEntity<>(JSON.toJSONString(resp), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException | TimeoutException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, String.format("query timeout after %dms.", QUERY_TIMEOUT_MS)),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
