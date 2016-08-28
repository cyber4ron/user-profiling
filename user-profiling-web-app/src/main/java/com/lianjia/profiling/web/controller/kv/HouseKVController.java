package com.lianjia.profiling.web.controller.kv;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

import static com.lianjia.profiling.web.common.Constants.HOUSE_IDX;
import static com.lianjia.profiling.web.common.Constants.HOUSE_TYPE;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

@RestController
public class HouseKVController {
    private static final Logger LOG = LoggerFactory.getLogger(UserKVController.class.getName());
    private static final int QUERY_TIMEOUT_MS = 5000;

    private TransportClient client;

    public HouseKVController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    private Map<String, Object> internalGetHouseOffline(String id) {
        GetRequestBuilder req = new GetRequestBuilder(client, GetAction.INSTANCE)
                .setIndex(HOUSE_IDX)
                .setType(HOUSE_TYPE)
                .setId(id);

        GetResponse resp = req.get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));
        return resp.getSourceAsMap();
    }

    @RequestMapping("/kv/house/offline/{id}")
    @CrossOrigin
    public ResponseEntity<String> getOfflineHouse(@PathVariable("id") String id,
                                                  @RequestParam(value = "token", required = false, defaultValue = "") String token) {
        try {
            if (!AccessManager.checkKV(token)) throw new IllegalAccessException();

            long startMs = System.currentTimeMillis();
            String json = JSON.toJSONString(Collections.singletonMap("data",
                                                                     internalGetHouseOffline(id)));
            LOG.info("process time: " + (System.currentTimeMillis() - startMs));

            return new ResponseEntity<>(json, HttpStatus.OK);

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
