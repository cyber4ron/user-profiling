package com.lianjia.profiling.web.controller.search;

import com.alibaba.druid.sql.parser.ParserException;
import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.dao.OlapDao;
import com.lianjia.profiling.web.domain.Request;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static com.lianjia.profiling.web.common.Constants.*;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

@RestController
public class UserSearchController {
    private static final Logger LOG = LoggerFactory.getLogger(UserSearchController.class.getName());

    private TransportClient client;

    public UserSearchController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", com.lianjia.profiling.util.Properties.get("es.cluster.name")).build();
        String[] nodes = com.lianjia.profiling.util.Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }
    private ResponseEntity<String> doOfflineSearch(Request.SearchRequest query) {
        return doOfflineSearch(query, CUST_IDX);
    }

    private ResponseEntity<String> doOfflineSearch(Request.SearchRequest query, String indexName) {
        try {
            if ((query.query == null || query.query.isEmpty()) && (query.sql == null || query.sql.isEmpty()))
                return new ResponseEntity<>(RespHelper.getFailResponse(1, "invalid arguments."),
                                            HttpStatus.BAD_REQUEST);

            if (!AccessManager.checkSearch(query.token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            List<Map<String, Object>> hits;
            if (!query.query.isEmpty()) {
                QueryBuilder qb = QueryBuilders.queryStringQuery(URLDecoder.decode(query.query, "UTF-8"));
                SearchRequestBuilder req = client.prepareSearch()
                        .setIndices(indexName)
                        .setQuery(qb)
                        .setFrom(query.pageNo * query.pageSize)
                        .setSize(query.pageSize);

                hits = Arrays.asList(req.get().getHits().getHits()).stream()
                        .map(SearchHit::getSource)
                        .collect(Collectors.toList());
            } else {
                hits = OlapDao.runQuery(query.sql);
            }

            String json = RespHelper.getTimedPagedListResp(hits, query.pageNo, query.pageSize, System.currentTimeMillis() - startMs);

            LOG.info(String.format("process time: %d, hits: %d",
                                   System.currentTimeMillis() - startMs, hits.size()));

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."),
                                        HttpStatus.UNAUTHORIZED);
        } catch (RejectedExecutionException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query rejected."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (UnsupportedEncodingException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "unsupported encoding."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException ex) {
            LOG.warn("", ex);
            if (ex.getCause() instanceof ParserException)
                return new ResponseEntity<>(RespHelper.getFailResponse(1, ex.getCause().getMessage()),
                                            HttpStatus.BAD_REQUEST);
            else return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                             HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SearchPhaseExecutionException ex) {
            LOG.warn("", ex);
            if (ex.getCause() instanceof QueryParsingException || ex.getCause() instanceof NotSerializableExceptionWrapper)
                return new ResponseEntity<>(RespHelper.getFailResponse(1, ex.getCause().getMessage()),
                                            HttpStatus.BAD_REQUEST);
            else return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                             HttpStatus.INTERNAL_SERVER_ERROR);
        }  catch (Exception ex) {
            LOG.warn("house search ex", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/search/user/offline")
    @CrossOrigin
    public ResponseEntity<String> searchOfflineUsers(@RequestParam(value = "query", required = false, defaultValue = "") String query,
                                                     @RequestParam(value = "sql", required = false, defaultValue = "") String sql,
                                                     @RequestParam(value = "page", required = false, defaultValue = "0") String page,
                                                     @RequestParam(value = "size", required = false, defaultValue = "200") String size,
                                                     @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        return doOfflineSearch(new Request.SearchRequest(query, sql, Integer.parseInt(page), Integer.parseInt(size), token));
    }

    @RequestMapping(value = "/search/user/offline", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> searchOfflineUsersPost(@RequestBody Request.SearchRequest query) {
        return doOfflineSearch(query);
    }

    @RequestMapping(value = "/search/user/offline/delegation", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getOlapUserDelegationOfflinePost(@RequestBody Request.SearchRequest query) {
        return doOfflineSearch(query, CUST_DEL_IDX);
    }

    @RequestMapping(value = "/search/user/offline/touring", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getOlapUserTouringOfflinePost(@RequestBody Request.SearchRequest query) {
        return doOfflineSearch(query, CUST_TOUR_IDX);
    }

    @RequestMapping(value = "/search/user/offline/contract", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getOlapUserContractOfflinePost(@RequestBody Request.SearchRequest query) {
        return doOfflineSearch(query, CUST_CONTRACT_IDX);
    }

    /* ---- online ---- */

    private ResponseEntity<String> doOnlineSearch(Request.UserOnlineSearchRequest query) {
        try {
            if ((query.query == null || query.query.isEmpty()) && (query.sql == null || query.sql.isEmpty()))
                return new ResponseEntity<>(RespHelper.getFailResponse(1, "invalid arguments."),
                                            HttpStatus.BAD_REQUEST);

            if (!AccessManager.checkSearch(query.token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            List<Map<String, Object>> hits;
            if (!query.query.isEmpty()) {
                String end = (query.end == null || query.end.isEmpty()) ? DateUtil.getDate() : query.end;
                String start = (query.start == null || query.start.isEmpty()) ?
                        DateUtil.dateDiff(DateUtil.getDate(), BACKTRACE_DAYS) :
                        query.start;

                List<String> indices = DateUtil.alignedByWeek(start, end).stream()
                        .map(date -> ONLINE_USER_IDX_PREFIX + date).collect(Collectors.toList());

                QueryBuilder qb = QueryBuilders.queryStringQuery(URLDecoder.decode(query.query, "UTF-8"));
                SearchRequestBuilder req = client.prepareSearch()
                        .setIndices(indices.toArray(new String[indices.size()]))
                        .setQuery(qb)
                        .setFrom(query.pageNo * query.pageSize)
                        .setSize(query.pageSize);

                hits = Arrays.asList(req.get().getHits().getHits()).stream()
                        .map(SearchHit::getSource)
                        .collect(Collectors.toList());
            } else {
                hits = OlapDao.runQuery(query.sql);
            }

            String json = RespHelper.getTimedPagedListResp(hits, query.pageNo, query.pageSize,
                                                           System.currentTimeMillis() - startMs);

            LOG.info(String.format("process time: %d, hits: %d",
                                   System.currentTimeMillis() - startMs, hits.size()));

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."),
                                        HttpStatus.UNAUTHORIZED);
        } catch (RejectedExecutionException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query rejected."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (UnsupportedEncodingException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "unsupported encoding."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException ex) {
            LOG.warn("", ex);
            if (ex.getCause() instanceof ParserException)
                return new ResponseEntity<>(RespHelper.getFailResponse(1, ex.getCause().getMessage()),
                                            HttpStatus.BAD_REQUEST);
            else return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                             HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SearchPhaseExecutionException ex) {
            LOG.warn("", ex);
            if (ex.getCause() instanceof QueryParsingException || ex.getCause() instanceof NotSerializableExceptionWrapper)
                return new ResponseEntity<>(RespHelper.getFailResponse(1, ex.getCause().getMessage()),
                                            HttpStatus.BAD_REQUEST);
            else return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                             HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * e.feat_sample. /user/online?query=_type:srh
     *
     * {
     * "sql" : "select ucid, ch_id from online_user_20160516 where _type = 'srh' group by ucid limit 15,20",
     * "token": "jeRPp5FAGN7vdSD9"
     * }
     */
    @RequestMapping("/search/user/online")
    @CrossOrigin
    public ResponseEntity<String> searchOnlineUsers(@RequestParam(value = "query", required = false, defaultValue = "") String query,
                                                    @RequestParam(value = "sql", required = false, defaultValue = "") String sql,
                                                    @RequestParam(value = "start", required = false, defaultValue = "") String start,
                                                    @RequestParam(value = "end", required = false, defaultValue = "") String end,
                                                    @RequestParam(value = "page", required = false, defaultValue = "0") String page,
                                                    @RequestParam(value = "size", required = false, defaultValue = "200") String size,
                                                    @RequestParam(value = "token", required = true) String token) {
        return doOnlineSearch(new Request.UserOnlineSearchRequest(query, sql, start, end,
                                                                  Integer.parseInt(page),
                                                                  Integer.parseInt(size),
                                                                  token));
    }

    @RequestMapping(value = "/search/user/online", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> searchOnlineUsersPost(@RequestBody Request.UserOnlineSearchRequest query) {
        return doOnlineSearch(query);
    }
}
