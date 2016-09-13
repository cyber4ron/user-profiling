package com.lianjia.profiling.web.controller.thridparty;

import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.util.FutureUtil;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author fenglei@lianjia.com on 2016-08
 */

@RestController
public class HouseOwnerSideController {
    private static final Logger LOG = LoggerFactory.getLogger(HouseEvalController.class.getName());
    private TransportClient client;

    public HouseOwnerSideController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    @RequestMapping("/house_online_stat/{houseId}")
    @CrossOrigin
    public ResponseEntity<String> getCustomerOffline(@PathVariable("houseId") String houseId) {
        try {
            List<CompletableFuture<Map<String, Long>>> futures = new ArrayList<>();

            futures.add(FutureUtil.getCompletableFuture(() -> {
                SearchResponse touringResp = client.prepareSearch("customer_touring").setTypes("touring")
                        .setQuery(QueryBuilders.matchQuery("house_id", houseId))
                        .addAggregation(AggregationBuilders.count("house_id").field("_index"))
                        .setSize(0).get();

                long touringCnt = ((InternalValueCount)(touringResp.getAggregations().get("house_id"))).getValue();

                return Collections.singletonMap("tr", touringCnt);
            }));

            List<String> days = DateUtil.alignedByWeek(DateUtil.getDaysBefore(7 * 13), DateUtil.getDate()).stream()
                    .map(date -> "online_user_" + date).collect(Collectors.toList());

            // long toTs = System.currentTimeMillis();
            // long fromTs = DateUtil.dateToTimestampMs(DateUtil.getDaysBefore(7 * 12));

            futures.add(FutureUtil.getCompletableFuture(() -> {
                SearchResponse dtlResp = client.prepareSearch(days.toArray(new String[days.size()]))
                        .setTypes("dtl", "mob_dtl")
                        .setQuery(QueryBuilders.matchQuery("dtl_id", houseId))
                        .addAggregation(AggregationBuilders.count("dtl_id").field("_index"))
                        .setSize(0).get();

                long dtlCnt = ((InternalValueCount)(dtlResp.getAggregations().get("dtl_id"))).getValue();
                return Collections.singletonMap("pv", dtlCnt);
            }));

            futures.add(FutureUtil.getCompletableFuture(() -> {
                                                SearchResponse followRealtimeResp = client.prepareSearch("online_user_" + DateUtil.alignedByWeek(DateUtil.getDate()))
                        .setTypes("fl", "mob_fl")
                        .setQuery(QueryBuilders.matchQuery("fl_id", houseId))
                        .setSize(10000)
                        .get();

                SearchResponse followHistoryResp = client.prepareSearch("db_follow").setTypes("fl")
                        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("target", houseId))
                                          .must(QueryBuilders.termQuery("status", 1)))
                        .setSize(10000)
                        .get();

                long flCnt = Stream.concat(Arrays.stream(followHistoryResp.getHits().hits()),
                                           Arrays.stream(followRealtimeResp.getHits().hits()))
                        .map(hit -> hit.getSource().getOrDefault("ucid", "unknown").toString())
                        .collect(Collectors.toSet()).size();

                return Collections.singletonMap("fl", flCnt);
            }));

            HashMap<String, Object> result = new HashMap<>();
            FutureUtil.sequence(futures).get(20, TimeUnit.SECONDS).stream().forEach(result::putAll);

            result.put("id", houseId);

            return new ResponseEntity<>(RespHelper.getSuccResponseForHouseOwnerSide(result), HttpStatus.OK);

        } catch (IllegalArgumentException ex) {
            return new ResponseEntity<>(RespHelper.getFailResponseForHouseOwnerSide(1, ex.getMessage()),
                                        HttpStatus.NOT_FOUND);
        } catch (Exception ex) {
            return new ResponseEntity<>(RespHelper.getFailResponseForHouseOwnerSide(1, "internal query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
