package com.lianjia.profiling.web.dao;

import com.lianjia.profiling.common.elasticsearch.ClientFactory;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.QueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class OlapDao {
    private static final Logger LOG = LoggerFactory.getLogger(OlapDao.class.getName());

    private static final int CONCURRENT_JOBS = 10;
    private static final int QUEUE_SIZE = 100;
    private static final long JOB_TIMEOUT_MS = 10 * 60 * 1000;
    private static final String LINE_SEP = "\t";
    private static final boolean INCLUDE_TYPE = false;
    private static final boolean INCLUDE_SCORE = false;
    private static final boolean IS_FLATTEN = true;

    private static final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private static ExecutorService service = new ThreadPoolExecutor(CONCURRENT_JOBS, CONCURRENT_JOBS,
                                                                    0L, TimeUnit.MILLISECONDS, queue);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOG.info("attempt to shutdown service");
                    service.shutdown();
                    service.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("tasks interrupted");
                } finally {
                    if (!service.isTerminated()) {
                        LOG.info("cancel non-finished tasks");
                    }
                    service.shutdownNow();
                    LOG.info("shutdown finished");
                }
            }
        });
    }

    private static Client client;

    public static int getQueueRemaining() {
        return queue.remainingCapacity();
    }

    static {
        try {
            client = ClientFactory.getClient();
        } catch (UnknownHostException e) {
            LOG.info("create sqlDao failed, ex: {}", e);
            System.exit(1);
        }

        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        String clusterName = nodeInfos.getClusterName().value();
        LOG.info("Found cluster... cluster name: {}", clusterName);
    }

    /**
     * Prepare action And transform sql
     * into ES ActionRequest
     *
     * @param sql SQL query to execute.
     * @return ES request
     * @throws SqlParseException
     */
    public static QueryAction explain(String sql) throws SqlParseException, SQLFeatureNotSupportedException {
        return ESActionFactory.create(client, sql);
    }

    private static List<Map<String, Object>> toList(CSVResult res) {
        List<String> headers = res.getHeaders();

        return res.getLines().stream().map(line -> {
            Map<String, Object> map = new HashMap<>();

            String[] fields = line.split(LINE_SEP);
            if (headers.size() != fields.length) return map;
            for (int i = 0; i < fields.length; i++) {
                Object field = fields[i];
                if (fields[i].startsWith("[") || fields[i].startsWith("{")) {
                    try {
                        field = fields[i];
                    } catch (Exception ex) {
                        // ex.printStackTrace();
                    }
                }
                map.put(headers.get(i), field);
            }

            return map;

        }).collect(Collectors.toList());
    }

    private static List<Map<String, Object>> extractResults(Object queryResult) throws CsvExtractorException {
        if (queryResult instanceof SearchHits) {
            SearchHit[] hits = ((SearchHits) queryResult).getHits();
            return Arrays.asList(hits).stream().map(SearchHit::sourceAsMap).collect(Collectors.toList());
        } else if (queryResult instanceof Aggregations) {
            CSVResult res = new CSVResultsExtractor(INCLUDE_TYPE, INCLUDE_SCORE).extractResults(queryResult, IS_FLATTEN, LINE_SEP);
            return toList(res);
        } else {
            return null;
        }
    }

    /**
     * run search or olap
     */
    public static List<Map<String, Object>> runQuery(String query) throws InterruptedException, ExecutionException, TimeoutException {
        Future<List<Map<String, Object>>> future = service.submit(() -> {
            QueryAction queryAction = explain(query);
            LOG.info("query explain: {}", queryAction.explain().explain());
            Object execution = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
            return extractResults(execution);
        });

        return future.get(JOB_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
