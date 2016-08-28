package com.lianjia.profiling.common.elasticsearch;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class ClientFactory {
    private static ConcurrentHashMap<String, Client> clientRegistry = new ConcurrentHashMap<>();
    private static Logger logger = LoggerFactory.getLogger(ClientFactory.class.getName(), Collections.<String, String>emptyMap());

    private ClientFactory() {
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    for (Client client : clientRegistry.values()) {
                        if (client != null)
                            client.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static Client getClient(String esCluster, String esHost, int esPort) throws UnknownHostException {
        String key = esCluster + "-" + esHost + "-" + Integer.toString(esPort);
        if (clientRegistry.containsKey(key)) return clientRegistry.get(key);
        else {
            logger.info(String.format("creating elasticsearch client, cluster name: %s, host: %s, port: %d", esCluster, esHost, esPort));
            Settings settings = Settings.settingsBuilder().put("cluster.name", esCluster).build();
            Client client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), esPort));

            Client oldClient;
            if ((oldClient = clientRegistry.putIfAbsent(key, client)) != null) {
                client.close();
                return oldClient;
            } else {
                return client;
            }
        }
    }

    public static Client getClient(String esCluster, List<String> nodes) throws UnknownHostException {
        String key = esCluster + '-' + StringUtils.join(nodes, '-');
        if (clientRegistry.containsKey(key)) return clientRegistry.get(key);
        else {
            logger.info(String.format("creating elasticsearch client, cluster name: %s, nodes: %s", esCluster, StringUtils.join(nodes, ',')));
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", esCluster)
                    .put("client.transport.ping_timeout", "60s").build();
            TransportClient client = TransportClient.builder()
                    .settings(settings)
                    .addPlugin(DeleteByQueryPlugin.class)
                    .build();
            for (String node : nodes) {
                String[] parts = node.split(":");
                if (parts.length != 2) continue;
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
            }

            Client oldClient;
            if ((oldClient = clientRegistry.putIfAbsent(key, client)) != null) {
                client.close();
                return oldClient;
            } else {
                return client;
            }
        }
    }

    public static Client getClient() throws UnknownHostException {
        String clusterName = Properties.getOrDefault("es.cluster.name", System.getenv("ES_NAME"));
        String clusterNodes = Properties.getOrDefault("es.cluster.nodes", System.getenv("ES_NODES"));

        if (clusterName == null || clusterNodes == null)
            throw new IllegalStateException("can not get cluster name or nodes");

        return getClient(clusterName, Arrays.asList(clusterNodes.split(",")));
    }
}

