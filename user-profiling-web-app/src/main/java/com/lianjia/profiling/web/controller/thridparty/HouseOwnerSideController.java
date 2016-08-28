package com.lianjia.profiling.web.controller.thridparty;

import com.lianjia.profiling.util.Properties;
import com.lianjia.profiling.web.util.RespHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
import java.util.Map;

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
            GetResponse response = client.prepareGet()
                    .setIndex("online_house_stat")
                    .setType("stat")
                    .setId(houseId)
                    .get();

            if (!response.isExists()) throw new IllegalArgumentException("house id not found.");

            Map<String, Object> result = response.getSourceAsMap();
            result.remove("date");
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
