package com.lianjia.profiling.web.controller.thridparty;

import com.lianjia.profiling.common.redis.JedisClient;
import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.dao.OlapDao;
import com.lianjia.profiling.web.domain.Request;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

@RestController
public class PopularHouseController {
    private static final Logger LOG = LoggerFactory.getLogger(PopularHouseController.class);
    private static final String IDX = "db_follow";
    private static final String TYPE = "fl";
    private static final long TIME_RANGE = 14 * 24 * 60 * 60 * 1000L;
    private static double touringPercentile = 0.05F;
    private static double followPercentile = 0.05F;

    private static final String sqlGetTourings = String.join("\n", "select house_id, bizcircle_id, count(*) as cnt",
                                                             "from customer_touring",
                                                             "where creation_date between '%start' and '%end'",
                                                             "group by house_id, bizcircle_id",
                                                             "limit 1000000");

    private static final String sqlGetHouseBizCircle = String.join("\n", "SELECT house_id, bizcircle_code",
                                                                   "FROM house",
                                                                   "where house_id = '%id'");

    private static final String sqlCountHouseTouring = String.join("\n", "SELECT house_id, bizcircle_id, count(*) as cnt",
                                                                   "FROM customer_touring",
                                                                   "where house_id = '%id'",
                                                                   "and creation_date between '%start' and '%end'");

    public static final String sqlGetFollow = String.join("\n", "SELECT target, city_id, count(*) as cnt",
                                                          "from db_follow where mtime between '%start' and '%end'",
                                                          "and city_id is not NULL",
                                                          "group by target, city_id",
                                                          "limit 1000000");

    private static final String sqlCountHouseFollowAll = String.join("\n", "SELECT count(distinct(target)) as cnt",
                                                                     "FROM db_follow",
                                                                     "where type = 'ershoufang' and status = 1",
                                                                     "and mtime between '%start' and '%end'");

    private static final String sqlCountHouseFollowThl = String.join("\n", "SELECT target, count(*) as cnt FROM db_follow",
                                                                     "where type = 'ershoufang' and status = 1",
                                                                     "and mtime between '%start' and '%end'",
                                                                     "group by target order by cnt desc limit %thd");

    private static final String sqlCountHouseFollow = String.join("\n", "SELECT count(*) as cnt",
                                                                  "FROM db_follow",
                                                                  "where target = '%id' and type = 'ershoufang' and status = 1",
                                                                  "and mtime between '%start' and '%end'");

    private static final String sqlGetHouseCity = String.join("\n", "SELECT house_id, city_id",
                                                              "FROM house",
                                                              "where house_id = '%id'");


    private volatile Map<String, Integer> touringThd;
    private volatile Map<String, Integer> followThd;

    private TransportClient client;

    public static class Tuple<X, Y> {
        public final X x;
        public final Y y;

        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    public PopularHouseController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();

        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }

        touringThd = new HashMap<>();

        new Timer(PopularHouseController.class.getName(), false).scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateThresholdInternal();
            }
        }, 0, 24 * 60 * 60 * 1000L);
    }

    private void updateThresholdInternal() {
        try {

            long timeMs = System.currentTimeMillis();
            String end = DateUtil.toDateTime(timeMs);
            String start = DateUtil.toDateTime(timeMs - TIME_RANGE);
            if (start == null || end == null) throw new IllegalArgumentException("start== null || end == null");

            // touring
            List<Map<String, Object>> tourings = OlapDao.runQuery(sqlGetTourings.replace("%start", start).replace("%end", end));

            Map<String, List<Tuple<String, Integer>>> bizCircleTourings = new HashMap<>();

            for (Map<String, Object> touring : tourings) {
                try {
                    String houseId = touring.get("house_id").toString();
                    String bizCircleId = touring.get("bizcircle_id").toString();
                    int count = (int) Float.parseFloat(touring.get("cnt").toString());

                    if (!bizCircleTourings.containsKey(bizCircleId))
                        bizCircleTourings.put(bizCircleId, new ArrayList<>());

                    bizCircleTourings.get(bizCircleId).add(new Tuple<>(houseId, count));

                } catch (Exception ex) {
                    // System.out.println(touring.toString());
                }
            }

            // sort
            bizCircleTourings.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                                              e -> {
                                                  Collections.sort(e.getValue(), (o1, o2) -> o2.y - o1.y);
                                                  return e.getValue();
                                              }));

            Map<String, Integer> newTouringThd = new HashMap<>();
            bizCircleTourings.forEach((k, v) -> newTouringThd.put(k, v.get(Math.max(0, (int) (v.size() * touringPercentile) - 1)).y));

            touringThd = newTouringThd;

            System.out.println("touring threshold updated, touringThd: " + touringThd);

            // follow
            Map<String, List<Tuple<String, Integer>>> cityFollows = new HashMap<>();

            List<Map<String, Object>> follows = OlapDao.runQuery(sqlGetFollow.replace("%start", start).replace("%end", end));

            for (Map<String, Object> follow : follows) {
                try {
                    String houseId = follow.get("target").toString();
                    String cityId = follow.get("city_id").toString();
                    int count = (int) Float.parseFloat(follow.get("cnt").toString());

                    if (!cityFollows.containsKey(cityId))
                        cityFollows.put(cityId, new ArrayList<>());

                    cityFollows.get(cityId).add(new Tuple<>(houseId, count));

                } catch (Exception ex) {
                    LOG.warn(follow.toString());
                }
            }

            // sort
            cityFollows.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                                              e -> {
                                                  Collections.sort(e.getValue(), (o1, o2) -> o2.y - o1.y);
                                                  return e.getValue();
                                              }));

            Map<String, Integer> newFollowThd = new HashMap<>();
            cityFollows.forEach((k, v) -> newFollowThd.put(k, v.get(Math.max(0, (int) (v.size() * followPercentile) - 1)).y));

            followThd = newFollowThd;

            System.out.println("follow threshold updated, followThd: " + followThd);

        } catch (InterruptedException | ExecutionException | TimeoutException | IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    private void updateThresholdInternal(double showingPct, double followPct) {
        touringPercentile = showingPct;
        followPercentile = followPct;

        updateThresholdInternal();
    }

    @Deprecated
    private Map<String, Object> getByFollowInternal(String houseId) throws InterruptedException, ExecutionException, TimeoutException {

        long timeMs = System.currentTimeMillis();
        String end = DateUtil.toDateTime(timeMs);
        String start = DateUtil.toDateTime(timeMs - TIME_RANGE);

        if (start == null || end == null) throw new IllegalArgumentException("start== null || end == null");

        String sqlHouseCity = sqlGetHouseCity.replace("%id", houseId);
        String sql = sqlCountHouseFollow.replace("%id", houseId).replace("%start", start).replace("%end", end);

        LOG.info(sqlHouseCity);
        LOG.info(sql);

        List<Map<String, Object>> resCity = OlapDao.runQuery(sqlHouseCity);
        List<Map<String, Object>> res = OlapDao.runQuery(sql);

        if (resCity.isEmpty() || res.isEmpty()) return Collections.singletonMap("house_id", houseId);

        String cityId = resCity.get(0).get("city_id").toString();

        Map<String, Object> resp = new HashMap<>();
        resp.put("house_id", houseId);
        resp.put("cnt", (int) Float.parseFloat(res.get(0).get("cnt").toString()));
        resp.put("thd", followThd.containsKey(cityId) ? followThd.get(cityId) : followThd.get("110000"));

        return resp;
    }

    private List<Map<String, Object>> getByFollowInternalRedisBatch(List<String> houseIds) {

        int tryDaysBefore = 5;
        int tries = 0;

        Map<String, Long> counts = new HashMap<>();
        DateTime date = new DateTime();

        while (counts.isEmpty() && tries < tryDaysBefore) {
            final int triesFinal = tries;
            counts = JedisClient.multiGetLong(houseIds.stream().map(houseId -> String.format("%s_%s_%s", "fl", houseId, DateUtil.toFormattedDate(date.minusDays(triesFinal))))
                                                      .collect(Collectors.toList()).toArray(new String[houseIds.size()]));
            tries++;
        }

        Map<String, String> cities = JedisClient.multiGetString(houseIds.stream().map(houseId -> String.format("%s_%s", "info", houseId))
                                                                        .collect(Collectors.toList()).toArray(new String[houseIds.size()]));

        List<Map<String, Object>> resp = new ArrayList<>();
        for (String houseId : houseIds) {
            Map<String, Object> map = new HashMap<>();
            map.put("house_id", houseId);
            map.put("cnt", counts.containsKey(houseId) ? counts.get(houseId) : -1);

            String cityId = "";
            if (cities.containsKey(houseId)) {
                String[] parts = cities.get(houseId).split("\t");
                if (parts.length == 2) {
                    cityId = parts[0];
                }
            }

            map.put("thd", followThd.containsKey(cityId) ? followThd.get(cityId) : followThd.get("110000"));

            resp.add(map);
        }

        return resp;
    }

    @Deprecated
    private Map<String, Object> getByTouringInternal(String houseId) throws InterruptedException, ExecutionException, TimeoutException {
        long timeMs = System.currentTimeMillis();
        String end = DateUtil.toDateTime(timeMs);
        String start = DateUtil.toDateTime(timeMs - TIME_RANGE);

        if (start == null || end == null) throw new IllegalArgumentException("start== null || end == null");

        String sqlBizCircle = sqlGetHouseBizCircle.replace("%id", houseId).replace("%start", start).replace("%end", end);
        String sqlCount = sqlCountHouseTouring.replace("%id", houseId).replace("%start", start).replace("%end", end);

        System.out.println(sqlBizCircle);
        System.out.println(sqlCount);

        List<Map<String, Object>> resBizCircle = OlapDao.runQuery(sqlBizCircle);
        List<Map<String, Object>> resCount = OlapDao.runQuery(sqlCount);

        if (resBizCircle.isEmpty() || resCount.isEmpty()) return Collections.singletonMap("house_id", houseId);

        Map<String, Object> resp = new HashMap<>();
        resp.put("house_id", houseId);
        resp.put("cnt", (int) Float.parseFloat(resCount.get(0).get("cnt").toString()));

        if (resBizCircle.get(0).containsKey("bizcircle_code")) {
            resp.put("bizcircle_code", resBizCircle.get(0).get("bizcircle_code").toString());
            if (touringThd.containsKey(resBizCircle.get(0).get("bizcircle_code").toString()))
                resp.put("thd", touringThd.get(resBizCircle.get(0).get("bizcircle_code").toString()));
            else
                resp.put("thd", -1);
        }

        return resp;
    }

    private List<Map<String, Object>> getByTouringInternalRedisBatch(List<String> houseIds) {

        int tryDaysBefore = 5;
        int tries = 0;

        Map<String, Long> counts = new HashMap<>();
        DateTime date = new DateTime();

        while (counts.isEmpty() && tries < tryDaysBefore) {
            final int triesFinal = tries;
            counts = JedisClient.multiGetLong(houseIds.stream().map(houseId -> String.format("%s_%s_%s", "tr", houseId, DateUtil.toFormattedDate(date.minusDays(triesFinal))))
                                                      .collect(Collectors.toList()).toArray(new String[houseIds.size()]));
            tries++;
        }

        Map<String, String> bizCircles = JedisClient.multiGetString(houseIds.stream().map(houseId -> String.format("%s_%s", "info", houseId))
                                                                            .collect(Collectors.toList()).toArray(new String[houseIds.size()]));

        List<Map<String, Object>> resp = new ArrayList<>();
        for (String houseId : houseIds) {
            Map<String, Object> map = new HashMap<>();
            map.put("house_id", houseId);
            map.put("cnt", counts.containsKey(houseId) ? counts.get(houseId) : -1);

            String bizCircleId = "";
            if (bizCircles.containsKey(houseId)) {
                String[] parts = bizCircles.get(houseId).split("\t");
                if (parts.length == 2) bizCircleId = parts[1];
            }

            map.put("thd", touringThd.containsKey(bizCircleId) ? touringThd.get(bizCircleId) : -1);

            resp.add(map);
        }

        return resp;
    }

    /**
     * e.feat_sample. /updatePopularHousePercentile?showingPct=0.1&followPct=0.1&token=jeRPp5FAGN7vdSD9
     */
    @RequestMapping("/updatePopularHousePercentile")
    @CrossOrigin
    public ResponseEntity<String> updateThreshold(@RequestParam(value = "showingPct", required = true, defaultValue = "") String showingPct,
                                                  @RequestParam(value = "followPct", required = true, defaultValue = "") String followPct,
                                                  @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            updateThresholdInternal(Double.parseDouble(showingPct), Double.parseDouble(followPct));

            Map<String, Object> resp = new HashMap<>();
            resp.put("showingPct", touringPercentile);
            resp.put("followPct", followPercentile);

            return new ResponseEntity<>(RespHelper.getSuccResp(resp),
                                        HttpStatus.OK);

        } catch (IllegalArgumentException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, "auth failed"),
                                        HttpStatus.UNAUTHORIZED);
        }
    }

    @RequestMapping("/follow/{houseId}")
    @CrossOrigin
    public ResponseEntity<String> getByFollow(@PathVariable("houseId") String houseId,
                                              @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();
            Map resp = getByFollowInternal(houseId);
            if (resp.size() < 3) throw new IllegalStateException("house id not found.");
            return new ResponseEntity<>(RespHelper.getSuccResponseForPopularHouse(resp),
                                        HttpStatus.OK);

        } catch (InterruptedException | ExecutionException | TimeoutException | IllegalArgumentException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, "auth failed"),
                                        HttpStatus.UNAUTHORIZED);
        }
    }

    @RequestMapping("/showing/{houseId}")
    @CrossOrigin
    public ResponseEntity<String> getByTouring(@PathVariable("houseId") String houseId,
                                               @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();
            Map<String, Object> resp = getByFollowInternal(houseId);
            if (resp.size() < 4) throw new IllegalStateException("house id not exist.");

            return new ResponseEntity<>(RespHelper.getSuccResponseForPopularHouse(resp),
                                        HttpStatus.OK);
        } catch (InterruptedException | ExecutionException | TimeoutException | IllegalArgumentException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, "auth failed"),
                                        HttpStatus.UNAUTHORIZED);
        }
    }

    @RequestMapping(path = "/follow", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getByFollowBatch(@RequestBody Request.BatchKVRequest request) {
        try {
            if (!AccessManager.checkKV(request.token)) throw new IllegalAccessException();
            if (request.ids.size() > 100) throw new IllegalArgumentException("too many ids");

            List<Map<String, Object>> resp = getByFollowInternalRedisBatch(request.ids);

            return new ResponseEntity<>(RespHelper.getSuccResponseForPopularHouse(resp),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, "auth failed"),
                                        HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(path = "/showing", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> getByTouringBatch(@RequestBody Request.BatchKVRequest request) {
        try {
            if (!AccessManager.checkKV(request.token)) throw new IllegalAccessException();
            if (request.ids.size() > 100) throw new IllegalArgumentException("too many ids");

            List<Map<String, Object>> resp = getByTouringInternalRedisBatch(request.ids);

            return new ResponseEntity<>(RespHelper.getSuccResponseForPopularHouse(resp),
                                        HttpStatus.OK);
        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, "auth failed"),
                                        HttpStatus.UNAUTHORIZED);
        } catch (IllegalArgumentException e) {
            LOG.warn("", e);
            return new ResponseEntity<>(RespHelper.getFailResponseForPopularHouse(1, e.getMessage()),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
