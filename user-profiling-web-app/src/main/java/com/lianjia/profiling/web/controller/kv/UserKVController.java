package com.lianjia.profiling.web.controller.kv;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.common.Constants.*;
import com.lianjia.profiling.web.dao.HBaseDao;
import com.lianjia.profiling.web.util.FutureUtil;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.lianjia.profiling.web.common.Constants.*;
import static com.lianjia.profiling.web.common.Dicts.ID_MAP;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

/**
 * perf test:
 * <p/>
 * 90 days, term query:
 * query time: 432
 * process time: 651
 * query time: 160
 * process time: 193
 * query time: 316
 * process time: 340
 * <p/>
 * 30 days, term query:
 * query time: 86
 * process time: 280
 * query time: 34
 * process time: 62
 * query time: 32
 * process time: 52
 * <p/>
 * 30 days, boolean query:
 * query time: 301
 * process time: 488
 * query time: 204
 * process time: 233
 * query time: 242
 * process time: 263
 * query time: 248
 * process time: 266
 */

@RestController
public class UserKVController {
    private static final Logger LOG = LoggerFactory.getLogger(UserKVController.class.getName());

    private TransportClient client;
    private HBaseDao hbaseDao;

    public UserKVController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        hbaseDao = new HBaseDao();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    public static Map<String, Object> rewriteFields(Map<String, Object> source) {
        return source.entrySet().stream().filter(entry -> !entry.getValue().toString().isEmpty())
                .collect(Collectors.toMap(entry -> getFieldValue(entry.getKey()), entry -> {
                    try {
                        String key = entry.getKey();
                        if (ID_MAP.containsKey(key))
                            return ID_MAP.get(key).apply(Integer.parseInt(entry.getValue().toString()));
                        else if (key.equals("ts"))
                            return DateUtil.toApiDateTime(Long.parseLong(entry.getValue().toString()));
                        else if (key.equals("event")) return getEventVal(entry.getValue().toString());
                        else return entry.getValue();
                    } catch (Exception ex) {
                        return entry.getValue();
                    }
                }));
    }

    public static List<Map<String, Object>> enrichOnlineUserEvent(List<Map<String, Object>> events) {
        return events.stream().map(event -> {
            try {
                return rewriteFields(event);
            } catch (Exception e) {
                return null;
            }
        }).filter(evt -> evt != null).collect(Collectors.toList());
    }

    /**
     * @param fieldName ucid or uuid
     */
    private List<SearchHit> dailyEventsQuery(String start, String end, int from, int size,
                                             String fieldName, Set<String> ids, long startTs, long endTs) {

        if(ids.isEmpty()) return Collections.emptyList();

        long beforeQueryMs = System.currentTimeMillis();

        MultiSearchRequestBuilder multiSearch = new MultiSearchRequestBuilder(client, MultiSearchAction.INSTANCE);
        ids.forEach(id -> {
            SearchRequestBuilder search = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(DateUtil.alignedByWeek(start, end).stream()
                                        .map(date -> ONLINE_USER_IDX_PREFIX + date)
                                        .toArray(String[]::new))
                    .setTypes(ONLINE_EVENTS.stream().map(Event::abbr).toArray(String[]::new))
                    .setFrom(from)
                    .setSize(size);

            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(fieldName, id))
                    .must(QueryBuilders.rangeQuery("ts").from(startTs).to(endTs));

            search.setQuery(boolQuery);

            multiSearch.add(search);
        });

        MultiSearchResponse resp = multiSearch.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

        System.out.println("query time: " + (System.currentTimeMillis() - beforeQueryMs));

        return Arrays.asList(resp.getResponses()).stream()
                .filter(item -> !item.isFailure())
                .flatMap(item -> Arrays.asList(item.getResponse().getHits().getHits()).stream())
                .collect(Collectors.toList());
    }

    private List<SearchHit> multiIndicesQuery(String start, String end, int from, int size,
                                              String ucid, long startTs, long endTs,
                                              boolean sorted) {
        long beforeQueryMs = System.currentTimeMillis();
        SearchRequestBuilder search = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setIndices(DateUtil.alignedByWeek(start, end).stream()
                                    .map(date -> ONLINE_USER_IDX_PREFIX + date)
                                    .toArray(String[]::new))
                .setTypes(ONLINE_EVENTS.stream().map(Event::abbr).toArray(String[]::new))
                .setFrom(from)
                .setSize(size);

        if (sorted) search.addSort("ts", SortOrder.DESC);

        TermQueryBuilder termQuery = QueryBuilders.termQuery("ucid", ucid);
        SearchResponse resp = search.setQuery(termQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

        // 较慢
        // BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
        //         .must(QueryBuilders.termQuery("ucid", ucid))
        //         .must(QueryBuilders.rangeQuery("ts").from(startTs).to(endTs));
        // SearchResponse resp = search.setQuery(boolQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

        System.out.println("query time: " + (System.currentTimeMillis() - beforeQueryMs));

        return Arrays.asList(resp.getHits().getHits());
    }

    /**
     * 不方便分页
     */
    private List<SearchHit> futureQuery(String start, String end, int from, int size,
                                        String ucid, long startTs, long endTs, boolean sorted) {

        end = end.isEmpty() ? DateUtil.getDate() : end;
        start = start.isEmpty() ? DateUtil.dateDiff(DateUtil.getDate(), BACKTRACE_DAYS) : start;

        List<CompletableFuture<List<SearchHit>>> futures = new ArrayList<>();

        DateUtil.alignedByWeek(start, end).forEach(date -> ONLINE_EVENTS.forEach(event ->
            futures.add(FutureUtil.getCompletableFuture(() -> {
                try {
                    SearchRequestBuilder search = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                            .setIndices(ONLINE_USER_IDX_PREFIX + date)
                            .setTypes(event.abbr())
                            .setFrom(from)
                            .setSize(size);

                    if (sorted)
                        search.addSort("ts", SortOrder.DESC);


                    TermQueryBuilder termQuery = QueryBuilders.termQuery("ucid", ucid);
                    SearchResponse resp = search.setQuery(termQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

                    // 较慢
                    // BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                    //         .must(QueryBuilders.termQuery("ucid", ucid))
                    //         .must(QueryBuilders.rangeQuery("ts").from(startTs).to(endTs));
                    // SearchResponse resp = search.setQuery(boolQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

                    return Arrays.asList(resp.getHits().getHits());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return new ArrayList<>();
                }
            }))
        ));

        try {
            long startMs = System.currentTimeMillis();
            List<SearchHit> events = FutureUtil.sequence(futures).get(10, TimeUnit.SECONDS).stream()
                    .flatMap(Collection::stream).collect(Collectors.toList());
            System.out.println(System.currentTimeMillis() - startMs);

            return events;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }

        return new ArrayList<>();
    }


    /**
     * 24周之前的数据在hbase中
     *
     * @param ucid ucid
     */
    private List<Map<String, Object>> internalGetUserOnline(String ucid, String start, String end,
                                                            int from, int size, boolean sort) {
        end = end.isEmpty() ? DateUtil.getDate() : end;
        start = start.isEmpty() ? DateUtil.dateDiff(DateUtil.getDate(), BACKTRACE_DAYS) : start;

        Long endTs = DateUtil.dateToTimestampMs(end);
        Long startTs = DateUtil.dateToTimestampMs(start);

        if (endTs == null || startTs == null) throw new IllegalArgumentException("invalid date format.");
        endTs += 86400000;

        List<SearchHit> hits = multiIndicesQuery(start, end, from, size, ucid, startTs, endTs, sort);

        return hits.stream().map(hit -> {
            Map<String, Object> res = hit.sourceAsMap();
            res.put("event", hit.type());
            return res;
        }).collect(Collectors.toList());
    }

    /**
     * hbase table数据按月切分, ES index数据按周切分
     *
     * @param start unix time, seconds
     * @param end   unix time, seconds
     */
    @RequestMapping("/kv/user/online/{phone}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnline(@PathVariable("phone") String phone,
                                                @RequestParam(value = "start", required = false, defaultValue = "") String start,
                                                @RequestParam(value = "end", required = false, defaultValue = "") String end,
                                                @RequestParam(value = "page", required = false, defaultValue = "0") String page,
                                                @RequestParam(value = "size", required = false, defaultValue = "200") String size,
                                                @RequestParam(value = "sort", required = false, defaultValue = "false") String sort,
                                                @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long beforeProcessMs = System.currentTimeMillis();
            GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                    .setIndex(UCID_MAP_IDX)
                    .setId(phone);

            GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
            String ucid = resp.getSourceAsMap().get("ucid").toString();

            List<?> events = enrichOnlineUserEvent(internalGetUserOnline(ucid, start, end,
                                                                         Integer.parseInt(page) * Integer.parseInt(size),
                                                                         Integer.parseInt(size),
                                                                         Boolean.parseBoolean(sort)
            ));

            String json = RespHelper.getPagedListResp(events, Integer.parseInt(page), Integer.parseInt(size));

            LOG.info("process time: " + (System.currentTimeMillis() - beforeProcessMs));

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "invalid params."), HttpStatus.BAD_REQUEST);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/kv/user/online/{type}/{week}/{ucid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlineByType(@PathVariable("type") String type,
                                                      @PathVariable(value = "week") String week,
                                                      @PathVariable("ucid") String ucid,
                                                      @RequestParam(value = "page", required = false, defaultValue = "0") String page,
                                                      @RequestParam(value = "size", required = false, defaultValue = "200") String size,
                                                      @RequestParam(value = "sort", required = false, defaultValue = "false") String sort,
                                                      @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();
            List<String> indices = Collections.singletonList(ONLINE_USER_IDX_PREFIX + DateUtil.alignedByWeek(week));

            SearchRequestBuilder search = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(indices.toArray(new String[indices.size()]))
                    .setTypes(type)
                    .setFrom(Integer.parseInt(page) * Integer.parseInt(size))
                    .setSize(Integer.parseInt(size));

            if (Boolean.parseBoolean(sort)) search.addSort("ts", SortOrder.DESC);

            TermQueryBuilder termQuery = QueryBuilders.termQuery("ucid", ucid);
            SearchResponse resp = search.setQuery(termQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

            System.out.println(String.format("query time: %d, hits: %d", System.currentTimeMillis() - startMs,
                                             resp.getHits().getHits().length));

            List<Map<String, Object>> data = Arrays.asList(resp.getHits().getHits()).stream()
                    .map(SearchHit::sourceAsMap).collect(Collectors.toList());

            String json = RespHelper.getPagedListResp(data, Integer.parseInt(page), Integer.parseInt(size));

            LOG.info("process time: " + (System.currentTimeMillis() - startMs));

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "invalid params."), HttpStatus.BAD_REQUEST);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping("/kv/user/online/{type}/{ucid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlineByType(@PathVariable("type") String type,
                                                      @PathVariable("ucid") String ucid,
                                                      @RequestParam(value = "start", required = false, defaultValue = "") String start,
                                                      @RequestParam(value = "end", required = false, defaultValue = "") String end,
                                                      @RequestParam(value = "page", required = false, defaultValue = "0") String page,
                                                      @RequestParam(value = "size", required = false, defaultValue = "200") String size,
                                                      @RequestParam(value = "sort", required = false, defaultValue = "false") String sort,
                                                      @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();

            end = end.isEmpty() ? DateUtil.getDate() : end;
            start = start.isEmpty() ? DateUtil.dateDiff(DateUtil.getDate(), BACKTRACE_DAYS) : start;

            List<String> indices = DateUtil.alignedByWeek(start, end).stream()
                    .map(date -> ONLINE_USER_IDX_PREFIX + date).collect(Collectors.toList());

            SearchRequestBuilder search = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(indices.toArray(new String[indices.size()]))
                    .setTypes(type)
                    .setFrom(Integer.parseInt(page) * Integer.parseInt(size))
                    .setSize(Integer.parseInt(size));

            if (Boolean.parseBoolean(sort)) search.addSort("ts", SortOrder.DESC);

            TermQueryBuilder termQuery = QueryBuilders.termQuery("ucid", ucid);
            SearchResponse resp = search.setQuery(termQuery).get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

            LOG.info(String.format("query time: %d, hits: %d", System.currentTimeMillis() - startMs,
                                   resp.getHits().getHits().length));

            List<Map<String, Object>> data = Arrays.asList(resp.getHits().getHits()).stream()
                    .map(SearchHit::sourceAsMap).collect(Collectors.toList());

            String json = RespHelper.getPagedListResp(data, Integer.parseInt(page), Integer.parseInt(size));

            LOG.info("process time: " + (System.currentTimeMillis() - startMs));

            return new ResponseEntity<>(json, HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "invalid params."), HttpStatus.BAD_REQUEST);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /* ----- offline ----- */

    @SuppressWarnings("Duplicates")
    private Map<String, Object> internalGetUserOffline(String id) {
        GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                .setIndex(CUST_IDX)
                .setType(CUST_TYPE)
                .setId(id);

        long startMs = System.currentTimeMillis();
        GetResponse resp = req.get(TimeValue.timeValueMillis(5000));
        System.out.println(System.currentTimeMillis() - startMs);

        return resp.getSourceAsMap();
    }

    @SuppressWarnings({"unchecked", "Convert2streamapi"})
    @RequestMapping("/kv/user/offline/{phone}")
    @CrossOrigin
    public ResponseEntity<String> getUserOffline(@PathVariable("phone") String phone,
                                                 @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                 @RequestParam(value = "sort", required = false, defaultValue = "false") String sort) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            Map<String, Object> events = internalGetUserOffline(phone);

            if (sort.equalsIgnoreCase("true")) {
                Map<String, Object> eventsSorted = new HashMap<>();
                for (Map.Entry<String, Object> eventSet : events.entrySet()) {
                    if (eventSet.getKey().equals("tourings")) {
                        Collections.sort((List<Map<String, Object>>) eventSet.getValue(), (t1, t2) -> {
                            if (!t1.containsKey("begin_time") && !t2.containsKey("begin_time")) return 0;
                            else if (t1.containsKey("begin_time") && t2.containsKey("begin_time")) {
                                return ((String) t1.get("begin_time")).compareTo(((String) t2.get("begin_time")));
                            } else if (!t1.containsKey("begin_time")) return 1;
                            else return -1;
                        });

                        for (Map<String, Object> touring : (List<Map<String, Object>>) eventSet.getValue()) {
                            if (touring.containsKey("houses")) {
                                Collections.sort((List<Map<String, Object>>) touring.get("houses"), (h1, h2) -> {
                                    if (!h1.containsKey("creation_date") && !h2.containsKey("creation_date")) return 0;
                                    else if (h1.containsKey("creation_date") && h2.containsKey("creation_date")) {
                                        return ((String) h1.get("creation_date")).compareTo(((String) h2.get("creation_date")));
                                    } else if (!h1.containsKey("creation_date")) return 1;
                                    else return -1;
                                });
                            }
                        }

                        eventsSorted.put("tourings", eventSet.getValue());
                    }

                    if (eventSet.getKey().equals("delegations")) {
                        Collections.sort((List<Map<String, Object>>) eventSet.getValue(), (d1, d2) -> {
                            if (!d1.containsKey("creation_time") && !d2.containsKey("creation_time")) return 0;
                            else if (d1.containsKey("creation_time") && d2.containsKey("creation_time")) {
                                return ((String) d1.get("creation_time")).compareTo(((String) d2.get("creation_time")));
                            } else if (!d1.containsKey("creation_time")) return 1;
                            else return -1;
                        });

                        eventsSorted.put("delegations", eventSet.getValue());
                    }

                    if (eventSet.getKey().equals("contracts")) {
                        Collections.sort((List<Map<String, Object>>) eventSet.getValue(), (c1, c2) -> {
                            if (!c1.containsKey("deal_time") && !c2.containsKey("deal_time")) return 0;
                            else if (c1.containsKey("deal_time") && c2.containsKey("deal_time")) {
                                return ((String) c1.get("deal_time")).compareTo(((String) c2.get("deal_time")));
                            } else if (!c1.containsKey("deal_time")) return 1;
                            else return -1;
                        });

                        eventsSorted.put("contracts", eventSet.getValue());
                    }

                    eventsSorted.put(eventSet.getKey(), eventSet.getValue());
                }

                events = eventsSorted;
            }

            return new ResponseEntity<>(JSON.toJSONString(Collections.singletonMap("data", events)),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String getUcidByUuid(String uuid) {
        GetResponse resp = new GetRequestBuilder(client, GetAction.INSTANCE)
                .setIndex("uuid_ucid_map")
                .setType("mp")
                .setId(uuid)
                .get();

        if (!resp.isExists()) return null;
        return resp.getSourceAsMap().get("ucid").toString();
    }

    public List<String> getUuidListByUcid(String ucid) {
        SearchResponse resp = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                .setIndices("uuid_ucid_map")
                .setTypes("mp")
                .setQuery(QueryBuilders.termQuery("ucid", ucid))
                .get();

        return Arrays.asList(resp.getHits().getHits()).stream()
                .map(hit -> hit.sourceAsMap().get("uuid").toString())
                .collect(Collectors.toList());
    }

    public Map<String, Map<String, Object>> getHouses(Set<String> houseIds) throws ExecutionException, InterruptedException {
        if (houseIds.isEmpty()) return Collections.emptyMap();

        MultiGetRequestBuilder multiGet = client.prepareMultiGet();
        houseIds.stream().forEach(id -> {
            multiGet.add("house", "house", id);
        });

        MultiGetResponse resp = multiGet.get();

        return Arrays.asList(resp.getResponses()).stream().filter(x -> x.getResponse().isExists())
                .map(x -> x.getResponse().getSourceAsMap())
                .collect(Collectors.toMap(x -> x
                        .get("house_id")
                        .toString(), x -> x));
    }

    /* ----- online, assoc uuid & ucid ----- */

    @SuppressWarnings("Duplicates")
    public List<Map<String, Object>> internalGetUserOnlineByUuid(String uuid, long startTs, long endTs, String sort) throws ExecutionException, InterruptedException {

        long startOfDay = new DateTime().withTimeAtStartOfDay().getMillis();

        List<Map<String, Object>> events = new ArrayList<>();
        if (endTs > startOfDay) {
            // search uuid
            final String[] ucid = new String[]{null};
            Set<String> houseIds = new HashSet<>();
            events = dailyEventsQuery(DateUtil.getDate(), DateUtil.getDate(), 0, 2000,
                                      "uuid", new HashSet<>(Collections.singletonList(uuid)),
                                      startTs, endTs).stream().map(hit -> {
                Map<String, Object> res = hit.sourceAsMap();

                if (res.containsKey("dtl_type") && res.get("dtl_type").toString().equals("1")
                        && res.containsKey("dtl_id")) houseIds.add(res.get("dtl_id").toString());

                if (res.containsKey("fl_type") && res.get("fl_type").toString().equals("2")
                        && res.containsKey("fl_id")) houseIds.add(res.get("fl_id").toString());

                if (res.containsKey("ucid")) ucid[0] = res.get("ucid").toString();

                res.put("evt", hit.type());
                return res;
            }).collect(Collectors.toList());

            // search associated ucid
            if (ucid[0] == null) ucid[0] = getUcidByUuid(uuid);
            if (ucid[0] != null) {
                // 2-degree associated uuids
                Set<String> uuids = new HashSet<>();
                events.addAll(dailyEventsQuery(DateUtil.getDate(), DateUtil.getDate(), 0, 2000,
                                               "ucid", new HashSet<>(Collections.singletonList(ucid[0])),
                                               startTs, endTs).stream().map(hit -> {
                    Map<String, Object> res = hit.sourceAsMap();

                    if (res.containsKey("dtl_type") && res.get("dtl_type").toString().equals("1")
                            && res.containsKey("dtl_id")) houseIds.add(res.get("dtl_id").toString());

                    if (res.containsKey("fl_type") && res.get("fl_type").toString().equals("2")
                            && res.containsKey("fl_id")) houseIds.add(res.get("fl_id").toString());

                    if (res.containsKey("uuid") && !res.get("uuid").toString().equals(uuid))
                        uuids.add(res.get("uuid").toString());

                    res.put("evt", hit.type());
                    return res;
                }).collect(Collectors.toList()));

                // 2-degree associated uuids
                events.addAll( dailyEventsQuery(DateUtil.getDate(), DateUtil.getDate(), 0, 2000,
                                                "uuid", uuids,
                                                startTs, endTs).stream().map(hit -> {
                    Map<String, Object> res = hit.sourceAsMap();

                    if (res.containsKey("dtl_type") && res.get("dtl_type").toString().equals("1")
                            && res.containsKey("dtl_id")) houseIds.add(res.get("dtl_id").toString());

                    if (res.containsKey("fl_type") && res.get("fl_type").toString().equals("2")
                            && res.containsKey("fl_id")) houseIds.add(res.get("fl_id").toString());

                    if (res.containsKey("ucid")) ucid[0] = res.get("ucid").toString();

                    res.put("evt", hit.type());
                    return res;
                }).collect(Collectors.toList()));
            }

            // get houses
            Map<String, Map<String, Object>> houses = getHouses(houseIds);
            events.forEach(evt -> {
                if (evt.containsKey("dtl_id") && houses.containsKey(evt.get("dtl_id").toString()))
                    evt.put("house", houses.get(evt.get("dtl_id").toString()));
                if (evt.containsKey("fl_id") && houses.containsKey(evt.get("fl_id").toString()))
                    evt.put("house", houses.get(evt.get("fl_id").toString()));
            });
        }

        // search uuid in hbase
        events.addAll(hbaseDao.getEvents(uuid, startTs, endTs));

        // search ucid in hbase
        String ucid = getUcidByUuid(uuid);
        if (ucid != null) events.addAll(hbaseDao.getEvents(ucid, startTs, endTs));

        List<Map<String, Object>> validEvents = events.stream()
                .filter(evt -> evt.containsKey("evt") && !evt.get("evt").toString().endsWith("usr"))
                .collect(Collectors.toList());

        // desc sort by ts
        if (sort.equalsIgnoreCase("true")) {
            Collections.sort(validEvents, (e1, e2) -> {
                if (!e1.containsKey("ts")) return 1;
                if (!e2.containsKey("ts")) return -1;
                return e2.get("ts").toString().compareTo(e1.get("ts").toString());
            });
        }

        return validEvents;
    }

    @SuppressWarnings("Duplicates")
    public List<Map<String, Object>> internalGetUserOnlineByUcid(String ucid, long startTs, long endTs, String sort) throws ExecutionException, InterruptedException {

        long startOfDay = new DateTime().withTimeAtStartOfDay().getMillis();

        List<Map<String, Object>> events = new ArrayList<>();

        Set<String> uuids = new HashSet<>();
        uuids.addAll(getUuidListByUcid(ucid));

        if (endTs > startOfDay) {
            Set<String> houseIds = new HashSet<>();
            events.addAll(dailyEventsQuery(DateUtil.getDate(), DateUtil.getDate(), 0, 2000,
                                      "ucid", new HashSet<>(Collections.singletonList(ucid)),
                                      startTs, endTs).parallelStream().map(hit -> {
                Map<String, Object> res = hit.sourceAsMap();

                if (res.containsKey("dtl_type") && res.get("dtl_type").toString().equals("1")
                        && res.containsKey("dtl_id")) houseIds.add(res.get("dtl_id").toString());

                if (res.containsKey("fl_type") && res.get("fl_type").toString().equals("2")
                        && res.containsKey("fl_id")) houseIds.add(res.get("fl_id").toString());

                if (res.containsKey("uuid")) uuids.add(res.get("uuid").toString());

                res.put("evt", hit.type());
                return res;
            }).collect(Collectors.toList()));


            // search associated uuids
            events.addAll(dailyEventsQuery(DateUtil.getDate(), DateUtil.getDate(), 0, 2000,
                                           "uuid", uuids,
                                           startTs, endTs).parallelStream().map(hit -> {
                Map<String, Object> res = hit.sourceAsMap();

                if (res.containsKey("dtl_type") && res.get("dtl_type").toString().equals("1")
                        && res.containsKey("dtl_id")) houseIds.add(res.get("dtl_id").toString());

                if (res.containsKey("fl_type") && res.get("fl_type").toString().equals("2")
                        && res.containsKey("fl_id")) houseIds.add(res.get("fl_id").toString());

                if (res.containsKey("uuid")) uuids.add(res.get("uuid").toString());

                res.put("evt", hit.type());
                return res;
            }).collect(Collectors.toList()));

            // get houses
            Map<String, Map<String, Object>> houses = getHouses(houseIds);
            events.parallelStream().forEach(evt -> {
                if (evt.containsKey("dtl_id") && houses.containsKey(evt.get("dtl_id").toString()))
                    evt.put("house", houses.get(evt.get("dtl_id").toString()));
                if (evt.containsKey("fl_id") && houses.containsKey(evt.get("fl_id").toString()))
                    evt.put("house", houses.get(evt.get("fl_id").toString()));
            });
        }

        // search ucid from hbase
        LOG.info("hbase query starting, ucid = " + ucid);
        long startMs = System.currentTimeMillis();
        events.addAll(hbaseDao.getEvents(ucid, startTs, endTs));

        // search uuids from hbase
        uuids.parallelStream().forEach(uuid -> events.addAll(hbaseDao.getEvents(uuid, startTs, endTs)));
        LOG.info("hbase query ended, ucid = " + ucid + ", query time = " + (System.currentTimeMillis() - startMs));

        List<Map<String, Object>> validEvents = events.parallelStream()
                .filter(evt -> evt.containsKey("evt") && !evt.get("evt").toString().endsWith("usr"))
                .collect(Collectors.toList());

        // desc sort by ts
        if (sort.equalsIgnoreCase("true")) {
            Collections.sort(validEvents, (e1, e2) -> {
                if (!e1.containsKey("ts")) return 1;
                if (!e2.containsKey("ts")) return -1;
                return e2.get("ts").toString().compareTo(e1.get("ts").toString());
            });
        }

        return validEvents;
    }

    @SuppressWarnings("Duplicates")
    @RequestMapping("/kv/user/assoc/online/uuid/{uuid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlineByUuid(@PathVariable("uuid") String uuid,
                                                       @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                       @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                       @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                       @RequestParam(value = "sort", required = false, defaultValue = "false") String sort) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            return new ResponseEntity<>(JSON.toJSONString(Collections.singletonMap("data",
                                                                                   internalGetUserOnlineByUuid(uuid, startTs, endTs, sort)),
                                                          SerializerFeature.DisableCircularReferenceDetect),
                                        HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @SuppressWarnings("Duplicates")
    @RequestMapping("/kv/user/assoc/online/ucid/{ucid}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlineByUcid(@PathVariable("ucid") String ucid,
                                                       @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                       @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                       @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                       @RequestParam(value = "sort", required = false, defaultValue = "false") String sort) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            List<Map<String, Object>> events = internalGetUserOnlineByUcid(ucid, startTs, endTs, sort);

            return new ResponseEntity<>(JSON.toJSONString(Collections.singletonMap("data", events),
                                                          SerializerFeature.DisableCircularReferenceDetect),
                                        HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @SuppressWarnings("Duplicates")
    @RequestMapping("/kv/user/assoc/online/phone/{phone}")
    @CrossOrigin
    public ResponseEntity<String> getUserOnlineByPhone(@PathVariable("phone") String phone,
                                                      @RequestParam(value = "startTs", required = false, defaultValue = "") String startTsStr,
                                                      @RequestParam(value = "endTs", required = false, defaultValue = "") String endTsStr,
                                                      @RequestParam(value = "token", required = false, defaultValue = "") String token,
                                                      @RequestParam(value = "sort", required = false, defaultValue = "false") String sort) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long endTs = endTsStr.isEmpty() ? System.currentTimeMillis() : Long.parseLong(endTsStr);
            long startTs = startTsStr.isEmpty() ? new DateTime(endTs).minusDays(BACKTRACE_DAYS).getMillis() : Long.parseLong(startTsStr);

            GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                    .setIndex(UCID_MAP_IDX)
                    .setId(phone);

            GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
            String ucid = resp.getSourceAsMap().get("ucid").toString();

            List<Map<String, Object>> events = internalGetUserOnlineByUcid(ucid, startTs, endTs, sort);

            return new ResponseEntity<>(JSON.toJSONString(Collections.singletonMap("data", events),
                                                          SerializerFeature.DisableCircularReferenceDetect),
                                        HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        } catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
