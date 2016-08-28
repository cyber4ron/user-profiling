package com.lianjia.profiling.common.elasticsearch.index;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.common.elasticsearch.ClientFactory;
import com.lianjia.profiling.util.Properties;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class IndexManager {
    private static Logger LOG = LoggerFactory.getLogger(IndexManager.class.getName());

    private Client client;

    Client getClient() throws UnknownHostException {
        String[] nodes = Properties.get("es.cluster.nodes").split(",");
        return getClient(Properties.get("es.cluster.name"), Arrays.asList(nodes));
    }

    Client getClient(String clusterName, List<String> esNodes) throws UnknownHostException {
        return ClientFactory.getClient(clusterName, esNodes);
    }

    public IndexManager(Map<String, String> conf) throws UnknownHostException {
        if (conf.containsKey("spark.es.cluster.nodes") && conf.containsKey("spark.es.cluster.name")) {
            String[] nodes = conf.get("spark.es.cluster.nodes").split(",");
            client = getClient(conf.get("spark.es.cluster.name"), Arrays.asList(nodes));
        } else {
            System.err.println("warn: missing es conf fields. use default.");
            client = getClient();
        }
    }

    public boolean exist(String indexName) {
        IndicesExistsResponse resp = client.admin().indices().prepareExists(indexName)
                .get();
        return resp.isExists();
    }

    public boolean remove(String indexName) {
        try {
            LOG.info("removing index: " + indexName);
            DeleteIndexResponse resp = client.admin().indices().prepareDelete(indexName).get();
            return resp.isAcknowledged();
        } catch (IndexNotFoundException e) {
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean create(String indexName, String jsonMapping) {
        CreateIndexResponse resp = client.admin().indices().prepareCreate(indexName)
                .setSource(jsonMapping)
                .get();

        return resp.isAcknowledged();
    }

    public boolean create(String indexName, String type, String jsonMapping) {
        CreateIndexResponse resp = client.admin().indices().prepareCreate(indexName)
                .addMapping(type, jsonMapping)
                .get();

        return resp.isAcknowledged();
    }

    public boolean create(String indexName, String jsonMapping, boolean clearFirst) {
        LOG.info("creating index: " + indexName + ", clearFirst: " + clearFirst);
        return clearFirst ? clearAndCreate(indexName, jsonMapping) : createIfNotExists(indexName, jsonMapping);
    }

    public boolean createIfNotExists(String indexName, String jsonMapping) {
        return exist(indexName) || create(indexName, jsonMapping);
    }

    public boolean clearAndCreate(String indexName, String jsonMapping) {
        return close(indexName) && remove(indexName) && create(indexName, jsonMapping);
    }

    public boolean close(String indexName) {
        try {
            CloseIndexResponse resp = client.admin().indices().close(new CloseIndexRequest(indexName)).get();
            return resp.isAcknowledged();
        } catch (ExecutionException e) {
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean open(String indexName) {
        try {
            OpenIndexResponse resp = client.admin().indices().open(new OpenIndexRequest(indexName)).get();
            return resp.isAcknowledged();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean updateSetting(String indexName, Map<String, ?> settings) {
        try {
            UpdateSettingsResponse resp = client.admin().indices()
                    .updateSettings(new UpdateSettingsRequest()
                                            .indices(indexName)
                                            .settings(settings))
                    .get();

            return resp.isAcknowledged();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean forceMerge(String indexName) {
        ForceMergeResponse resp = client.admin().indices().prepareForceMerge(indexName).get();
        return resp.getFailedShards() == 0;
    }

    public boolean setReplicas(String indexName, int replicaNum) {
        return updateSetting(indexName, Collections.singletonMap("number_of_replicas", replicaNum));
    }

    public boolean setRefreshInterval(String indexName, Object interval) {
        return updateSetting(indexName, Collections.singletonMap("refresh_interval", interval));
    }

    public boolean compactIndex(String indexName) {
        ForceMergeRequest req = new ForceMergeRequest(indexName).onlyExpungeDeletes(true);
        try {
            ForceMergeResponse resp = client.admin().indices().forceMerge(req).get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }
}
