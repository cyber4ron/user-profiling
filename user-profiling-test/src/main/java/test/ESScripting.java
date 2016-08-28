package test;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.kohsuke.args4j.CmdLineException;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class ESScripting extends ESBase {

//    // todo: BulkProcessor
    public void scroll() { // 最好后续处理pipeline, 或future?
        QueryBuilder qb = termQuery("balcony_need", "0");

        int counter = 0;

        SearchResponse scrollResp = client.prepareSearch("delegation2")
            .setTypes("delegation")
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setScroll(new TimeValue(60000))
            .setQuery(qb)
            .setSize(1000).execute().actionGet();

        while (true) {
            System.out.println("====>>>> counter: " + counter);
            BulkRequestBuilder rb = client.prepareBulk();
            Set<String> ids = new HashSet<>();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                // System.out.println(hit.getSource());
                ids.add(hit.getId());
            }

            for(String id: ids) {
//                UpdateRequest updateRequest = new UpdateRequest("delegation2", "delegation", id).version(9).versionType(VersionType.EXTERNAL_GTE)
//                    .script(new Script("ctx._version += 99"));

                IndexRequest iq = new IndexRequest("delegation2", "delegation", id).version(9).versionType(VersionType.EXTERNAL_GTE).opType(
                    IndexRequest.OpType.CREATE).source("{xxx}"); // clearAndCreate / index 两种optype
                rb.add(iq);
                counter ++;
            }

            BulkResponse bulkResp = rb.execute().actionGet();

            for (BulkItemResponse resp : bulkResp.getItems()) {
                if (resp == null) {
                    System.out.println("====> " + resp.getFailure());
                    System.out.println("====> " + resp.toString());
                    System.out.println("====> " + resp.getFailureMessage());
                    System.out.println("====> " + resp.getFailure().getCause());
                    System.out.println("====> " + resp.getFailure().getMessage());
                    continue;
                }

                System.out.println(resp.getId());
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                .setScroll(new TimeValue(60000)).execute().actionGet();

            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

    }

    @Override
    public void run() throws UnknownHostException, ExecutionException, InterruptedException, CmdLineException {

//        UpdateRequest updateRequest = new UpdateRequest("delegation2", "delegation", "AVN_6fQNreIDtqRdiTS2")
//            .script(new Script("ctx._source.bizcircle_name += \"测试\""));
//
//        client.update(updateRequest).get();

//        scroll();

        Map<String, Object> params = new HashMap<>();
        Map<String, String> dur = new HashMap<>();
        dur.put("duration", "12");
        Map<String, String> dur2 = new HashMap<>();
        dur2.put("duration", "13");
        List<Map<String, String>> durs = Arrays.asList(dur, dur2);
        params.put("dur", durs);

        // nested field append
        UpdateResponse resp = client.prepareUpdate("user", "user", "AVOetNYx2LpYwtzNLmI_")
            .setScript(new Script("ctx._source.mobile_durations+=dur", ScriptService.ScriptType.INLINE, null, params)).get();

        System.out.println(resp.getGetResult());

    }

    public static void main(String[] args) throws Exception {
        new ESScripting().parseArgs(args).createClient().run();
    }
}
