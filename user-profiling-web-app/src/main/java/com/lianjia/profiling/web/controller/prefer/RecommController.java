package com.lianjia.profiling.web.controller.prefer;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.HouseTag;
import com.lianjia.profiling.tagging.user.OfflineEventTagging;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.lianjia.profiling.web.common.Constants.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

@RestController
public class RecommController {
    private static final Logger LOG = LoggerFactory.getLogger(RecommController.class.getName());
    private static final int QUERY_TIMEOUT_MS = 5000;

    private TransportClient client;

    public RecommController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    @SuppressWarnings("Duplicates")
    private String internalGetUserOffline(String id) {
        GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                .setIndex(CUST_IDX)
                .setType(CUST_TYPE)
                .setId(id);

        long startMs = System.currentTimeMillis();
        GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
        System.out.println(System.currentTimeMillis() - startMs);

        return resp.getSourceAsString();
    }

    @RequestMapping("/recomm/user/{id}")
    @CrossOrigin
    public ResponseEntity<String> searchOfflineUsers(@PathVariable("id") String id,
                                                     @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();
            String json = internalGetUserOffline(id);
            UserPreference prefer = OfflineEventTagging.compute(JSON.parseObject(json));

            System.err.println(prefer);
            System.err.println(prefer.toReadableJson());

            BoolQueryBuilder query =  QueryBuilders.boolQuery();
            query.must(QueryBuilders.termQuery("status", "200100000001"));
            query.must(QueryBuilders.termQuery("biz_type", "200200000001"));
            for(HouseTag tag : HouseTag.values()) {
                if(prefer.getEntries().containsKey(tag.userTag())) {
                    if(UserPreference.isIdField(tag.userTag())) {
                        Object[][] prefers = ((Object[][]) prefer.getEntries().get(tag.userTag()));
                        String[] ids = new String[prefers.length];
                        for(int i = 0;i<prefers.length; i++) {
                            ids[i] = (String) prefers[i][0];
                        }

                        for(String x: ids) {
                            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(tag.val(), x);
                            query.should(matchQuery);
                            matchQuery.boost(tag.boost());
                        }
                    } else {
                        Object[][] prefers = ((Object[][]) prefer.getEntries().get(tag.userTag()));
                        Integer [] indices = new Integer[prefers.length];
                        for(int i = 0;i<prefers.length; i++) {
                            indices[i] = (Integer) prefers[i][0];
                        }

                        for (int x : indices) {
                            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(tag.val(), x);
                            query.should(matchQuery);
                            matchQuery.boost(tag.boost());
                        }
                    }
                }
            }

            SearchResponse resp =  new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices(HOUSE_PROP_IDX)
                    .setTypes(HOUSE_PROP_TYPE)
                    .setQuery(query)
                    .setSize(20)
                    .get();

            List<Map<String, Object>> res = new ArrayList<>();
            Collections.shuffle(res);
            for(SearchHit hit: resp.getHits().hits()) {
                Map<String, Object> item = new HashMap<>();
                item.put("house_id", hit.getId());
                item.put("score", hit.getScore());
                item.put("url", "http://bj.lianjia.com/ershoufang/" + hit.getId() + ".html");
                res.add(item);
            }

            LOG.info("process time: " + (System.currentTimeMillis() - startMs));
            return new ResponseEntity<>(JSON.toJSONString(Collections.singletonMap("data", res)),
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
