package com.lianjia.profiling;

import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.kohsuke.args4j.CmdLineException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class Fetch extends ESBase {

    public Fetch(String[] args) throws CmdLineException, UnknownHostException {
        super(args);
    }

    public NestedQueryBuilder nestedBoolQuery(final Map<String, String> propertyValues, final String nestedPath) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (String propertyName : propertyValues.keySet()) {
            String propertyValue = propertyValues.get(propertyName);
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(propertyName, propertyValue);
            boolQueryBuilder.must(matchQuery);
        }

        return QueryBuilders.nestedQuery(nestedPath, boolQueryBuilder);
    }

    void multiSearch() {
        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .execute().actionGet();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
        }
    }

    void aggregate() {
        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("agg1").field("field")
                )
                .addAggregation(
                        AggregationBuilders.dateHistogram("agg2")
                                .field("birth")
                                .interval(DateHistogramInterval.YEAR)
                )
                .execute().actionGet();

        // Get your facet results. todo facet is deprecated?
        Terms agg1 = sr.getAggregations().get("agg1");
        sr.getAggregations().get("agg2");
    }

    void structuringAggr() {
        SearchResponse sr = client.prepareSearch().addAggregation(AggregationBuilders.terms("by_country").field("country")
                                                                          .subAggregation(AggregationBuilders.dateHistogram("by_year")
                                                                                                  .subAggregation(AggregationBuilders.avg("avg_children").field("children"))
                                                                                                  .field("dateOfBirth")
                                                                                                  .interval((DateHistogramInterval.YEAR)))).execute().actionGet();
    }

    void percolate() throws IOException {
        //This is the query we're registering in the percolator
        QueryBuilder qb = termQuery("content", "amazing");

        //Index the query = register it in the percolator
        client.prepareIndex("myIndexName", ".percolator", "myDesignatedQueryName")
                .setSource("{}")
                .setRefresh(true) // Needed when the query shall be available immediately
                .execute().actionGet();

        //Build a document to check against the percolator
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("doc").startObject(); //This is needed to designate the document
        docBuilder.field("content", "This is amazing!");
        docBuilder.endObject(); //End of the doc field
        docBuilder.endObject(); //End of the JSON root object

        //Percolate
        PercolateResponse response = client.preparePercolate()
                .setIndices("myIndexName")
                .setDocumentType("myDocumentType")
                .setSource(docBuilder).execute().actionGet();

        //Iterate over the results
        for (PercolateResponse.Match match : response) {
            //Handle the result which is the name of
            //the query in the percolator
        }
    }

    void query() {
        QueryBuilder qb = QueryBuilders.functionScoreQuery()
                .add(QueryBuilders.matchQuery("name", "kimchy"),
                     randomFunction("ABCDEF"))
                .add(exponentialDecayFunction("age", 0L, 1L));
    }

    /**
     * The maximum number of documents to collect for each shard
     */
    void limit() {
        SearchResponse sr = client.prepareSearch("customer")
                .setTerminateAfter(1000)
                .get();

        if (sr.isTerminatedEarly()) {
            // We finished early
        }
    }

    public void scroll() throws IOException {
        QueryBuilder term = termQuery("phone", "13366355799");
        QueryBuilder wildcard = wildcardQuery("phone", "13366355799");

        Map<String, String> propertyValues = new HashMap<>();
        propertyValues.put("consultations.prescriptions", "alfuorism");
        propertyValues.put("consultations.Diagnosis", "Fever");
        nestedBoolQuery(propertyValues, "consultations");

        long startMs = System.currentTimeMillis();

        SearchResponse scrollResp = client.prepareSearch("customer")
                .setTypes("customer")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setScroll(new TimeValue(60000)) // todo: unit?
                .setQuery(wildcard)
                .setSize(1000).execute().actionGet();

        Path path = Paths.get("133");
        BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);


        int count = 0;
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                // System.out.println(hit.sourceAsString());
                writer.write(hit.sourceAsString());
                writer.newLine();
                count++;
                if (count % 10000 == 0) System.out.println(count);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

        writer.close();

        System.out.println(count);

        long endMs = System.currentTimeMillis();
        System.out.println(String.format("time: %dms", endMs - startMs));

    }

    public static void main(String[] args) throws CmdLineException, IOException {
        new Fetch(args).scroll();
    }
}
