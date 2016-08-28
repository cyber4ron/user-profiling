package com.lianjia.profiling.web.controller.thridparty;

import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */
@RestController
public class HouseEvalController {
    private static final Logger LOG = LoggerFactory.getLogger(HouseEvalController.class.getName());
    private TransportClient client;

    private static final int BASE = 2154573; // 20160627

    public HouseEvalController() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Properties.get("es.cluster.name")).build();
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        for (String node : nodes) {
            String[] parts = node.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
    }

    @RequestMapping("/house_value/total_usage")
    @CrossOrigin
    public String getCustomerOffline() {
        try {
            SearchResponse response = client.prepareSearch("house_eval")
                    .setTypes("ft")
                    .setSize(0)
                    .get();
            long count = BASE + response.getHits().totalHits();
            return RespHelper.getHouseEvalSuccResp(count);
        } catch (Exception ex) {
            return RespHelper.getHouseEvalFailedResp(1, "internal query failed.");
        }
    }
}
