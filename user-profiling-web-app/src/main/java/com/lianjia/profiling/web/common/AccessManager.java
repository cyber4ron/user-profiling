package com.lianjia.profiling.web.common;

import com.lianjia.profiling.web.util.EncryptUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class AccessManager {
    private static final Logger LOG = LoggerFactory.getLogger(AccessManager.class.getName());
    private static final Map<String, Privilege> PRIVILEGE_VALUE_MAP = new HashMap<>();
    private static Map<String, Account> accounts = new HashMap<>();
    private static TransportClient client;

    static {
        PRIVILEGE_VALUE_MAP.put("kv", Privilege.KV);
        PRIVILEGE_VALUE_MAP.put("search", Privilege.SEARCH);
        PRIVILEGE_VALUE_MAP.put("olap", Privilege.OLAP);
        PRIVILEGE_VALUE_MAP.put("write", Privilege.WRITE);

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", com.lianjia.profiling.util.Properties.get("es.cluster.name")).build();
        String[] nodes = com.lianjia.profiling.util.Properties.get("es.cluster.nodes").split(",");
        client = TransportClient.builder().settings(settings).build();
        try {
            for (String node : nodes) {
                String[] parts = node.split(":");
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        buildAccounts();
    }

    public enum Privilege {
        KV("kv"),
        SEARCH("search"),
        OLAP("olap"),
        WRITE("write");

        String value;

        public String value() {
            return value;
        }

        Privilege(String value) {
            this.value = value;
        }
    }

    public static class Account {
        private String token;
        private String name;
        private Set<Privilege> privileges = new HashSet<>();

        public Account(String token, String name, Map<String, Object> privilege) {
            this.token = token;
            this.name = name;
            for (Map.Entry<String, Object> e : privilege.entrySet()) {
                if (!PRIVILEGE_VALUE_MAP.containsKey(e.getKey())) continue;
                if (Boolean.parseBoolean(e.getValue().toString()))
                    privileges.add(PRIVILEGE_VALUE_MAP.get(e.getKey()));
            }
        }

        public boolean hasPrivilege(Privilege prv) {
            return privileges.contains(prv);
        }

        public Map<String, Object> getAccountAsMap() {
            Map<String, Object> acc = new HashMap<>();
            acc.put("token", token);
            acc.put("name", name);
            acc.put("privilege", privileges.stream().map(Privilege::value).collect(Collectors.toList()));
            return acc;
        }
    }

    public static void buildAccounts() {
        SearchResponse resp = client.prepareSearch()
                .setIndices("access")
                .setTypes("access")
                .get();

        Map<String, Account> newAccounts = Arrays.asList(resp.getHits().hits()).stream()
                .map(SearchHit::getSource)
                .collect(Collectors.toMap(acc -> EncryptUtil.decrypt(acc.get("token_encrypted").toString()),
                                          acc -> new Account(acc.get("name").toString(),
                                                             acc.get("token_encrypted").toString(),
                                                             acc)));
        accounts = newAccounts;
    }

    public static List<Map<String, Object>> getAccountAsMap() {
        return accounts.entrySet().stream().map(e -> e.getValue().getAccountAsMap()).collect(Collectors.toList());
    }

    public static boolean hasPrivilege(String token, Privilege prv) {
        return token != null && accounts.containsKey(token) && accounts.get(token).hasPrivilege(prv);
    }

    public static boolean checkKV(String token){
        return AccessManager.hasPrivilege(token, Privilege.KV);
    }

    public static boolean checkSearch(String token){
        return AccessManager.hasPrivilege(token, Privilege.SEARCH);
    }

    public static boolean checkOlap(String token){
        return AccessManager.hasPrivilege(token, Privilege.OLAP);
    }
}
