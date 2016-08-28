package com.lianjia.profiling.web.controller.kv;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.common.AccessManager;
import com.lianjia.profiling.web.domain.Request;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.MultiGetResponse;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.lianjia.profiling.web.common.Constants.HOUSE_IDX;
import static com.lianjia.profiling.web.common.Constants.HOUSE_TYPE;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

@RestController
public class HouseKVBatchController {
    private static final Logger LOG = LoggerFactory.getLogger(HouseKVBatchController.class.getName());
    private static final int QUERY_TIMEOUT_MS = 5000;

    private TransportClient client;

    public HouseKVBatchController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    private List<Map<String, Object>> offlineHouseMultiGet(List<String> phones) {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add(HOUSE_IDX, HOUSE_TYPE, phones)
                .get(TimeValue.timeValueMillis(QUERY_TIMEOUT_MS));

        return Arrays.asList(multiGetItemResponses.getResponses()).stream()
                .filter(x -> x.getResponse().isExists())
                .map(x -> x.getResponse().getSource())
                .collect(Collectors.toList());
    }

    /**
     * [
     * "BJxx1",
     * "BJxx2",
     * ]
     */

    @RequestMapping(value = "/kv/batch/house/offline", method = RequestMethod.POST)
    @CrossOrigin
    public ResponseEntity<String> batchGetOfflineHouse(@RequestBody Request.BatchKVRequest request) {
        try {
            if (!AccessManager.checkKV( request.token)) throw new IllegalAccessException();

            Map<String, Object> resp = new HashMap<>();
            List<?> list = offlineHouseMultiGet(request.ids);
            resp.put("data", list);
            resp.put("num", list.size());

            return new ResponseEntity<>(JSON.toJSONString(resp), HttpStatus.OK);

        } catch (IllegalAccessException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "auth failed."), HttpStatus.UNAUTHORIZED);
        }   catch (IllegalStateException ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query timeout."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception ex) {
            LOG.warn("", ex);
            return new ResponseEntity<>(RespHelper.getFailResponse(2, "query failed."),
                                        HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
