package test;

import org.apache.commons.cli.ParseException;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

/**
 * todo: es-hadoop/spark可以吗？
 */
public class ESJoin {
    private Settings nodeSettings(String pathHome) {
        return Settings.settingsBuilder()
            .put("path.home", pathHome)
            .build();
    }

    @Option(name = "--cluster-name")
    private String clusterName = "my-application";

    private void run(String[] args) throws CmdLineException, UnknownHostException {
        // https://github.com/kohsuke/args4j/blob/master/args4j/examples/SampleMain.java
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
        System.out.println(clusterName);

        Settings settings = Settings.settingsBuilder()
            .put("cluster.name", clusterName).build();
        Client client = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.30.17.2"), 9300));

        // scrolls
        QueryBuilder qb = termQuery("city_id", "110000");

        SearchResponse scrollResp = client.prepareSearch("touring")
            .setTypes("touring_house")
            .setSearchType(SearchType.QUERY_AND_FETCH) // 用SCAN的话while循环会进入两遍..?
            .setScroll(new TimeValue(600000))
            .setQuery(qb)
            .setSize(1000).execute().actionGet(); //100 hits per shard will be returned for each scroll
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-search-scrolling.html
        // todo: EsRejectedExecutionException[rejected execution of org.elasticsearch.transport.TransportService$4@2c13778f on EsThreadPoolExecutor[search, queue capacity = 1000, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@67c5ebe7[Running, pool size = 7, active threads = 7, queued tasks = 1000, completed tasks = 1799417]]]

        int counter = 0;

        //Scroll until no hits are returned
        while (true) {
            System.out.println("----> " + counter);

            System.out.println("in while.");

            Set<String> sl = new HashSet<>();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                sl.add(hit.getSource().get("house_id").toString());
            }

            MultiSearchRequestBuilder bd = client.prepareMultiSearch();
            for (String s : sl) {
                // System.out.println("s: " + s);
                bd.add(client.prepareSearch("house")
                    .setTypes("house")
                    .setQuery(QueryBuilders.matchQuery("house_id", s))
                    .setSize(1)); // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-search-msearch.html
                counter++;
            }

            if (sl.size() != 0) {
                MultiSearchResponse sr = bd.execute().actionGet();

                long nbHits = 0;
                for (MultiSearchResponse.Item item : sr.getResponses()) {
                    SearchResponse response = item.getResponse(); // 有时null
                    if (response == null) {
                        System.out.println("====> " + item.isFailure());
                        System.out.println("====> " + item.toString());
                        System.out.println("====> " + item.getFailureMessage());
                        System.out.println("====> " + item.getFailure().getCause());
                        System.out.println("====> " + item.getFailure().getLocalizedMessage());
                        System.out.println("====> " + item.getFailure().getMessage());
                        continue; // todo: why
                    }
                    nbHits += response.getHits().getTotalHits();
                    for (SearchHit hit : response.getHits().getHits()) {
                        System.out.println(hit.getId());
                    }
                }
            }

            // prepareSearchScroll
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                .setScroll(new TimeValue(60000)).execute().actionGet();

            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }

        }

        client.close();
    }

    public static void main(String[] args) throws IOException, ParseException, CmdLineException {
        new ESJoin().run(args);
    }
}
