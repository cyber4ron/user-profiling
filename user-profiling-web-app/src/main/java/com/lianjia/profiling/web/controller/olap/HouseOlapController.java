package com.lianjia.profiling.web.controller.olap;

import com.alibaba.druid.sql.parser.ParserException;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.dao.OlapDao;
import com.lianjia.profiling.web.domain.Request;
import com.lianjia.profiling.web.util.RespHelper;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

@RestController
public class HouseOlapController {
    private static final Logger LOG = LoggerFactory.getLogger(HouseOlapController.class.getName());

    private ResponseEntity<String> doOlap(Request.OlapRequest query) {
        try {
            if (!AccessManager.checkOlap(query.token)) throw new IllegalAccessException();
            if (query.sql == null || query.sql.isEmpty())
                return new ResponseEntity<>(RespHelper.getFailResponse(1, "invalid arguments."),
                                            HttpStatus.BAD_REQUEST);

            long startMs = System.currentTimeMillis();
            List<Map<String, Object>> resp = OlapDao.runQuery(query.sql);

            String json = RespHelper.getTimedListResp(resp, System.currentTimeMillis() - startMs);

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."),
                                        HttpStatus.UNAUTHORIZED);
        } catch (RejectedExecutionException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query rejected."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException ex) {
            LOG.warn("", ex);
            if (ex.getCause() instanceof ParserException || ex.getCause() instanceof SqlParseException)
                return new ResponseEntity<>(RespHelper.getFailResponse(1, ex.getCause().getMessage()),
                                            HttpStatus.BAD_REQUEST);
            else return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                             HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/olap/house/offline")
    @CrossOrigin
    public ResponseEntity<String> getOlapHouseOffline(@RequestParam(value = "sql", required = true) String sql,
                                                      @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        return doOlap(new Request.OlapRequest(sql, token));
    }

    @RequestMapping(value = "/olap/house/offline", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getOlapHouseOffline(@RequestBody Request.OlapRequest query) {
        return doOlap(query);
    }
}
