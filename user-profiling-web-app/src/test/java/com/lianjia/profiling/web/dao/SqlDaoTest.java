package com.lianjia.profiling.web.dao;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class SqlDaoTest {

    private OlapDao sqlDao;

    @Before
    public void setUp() throws Exception {
        sqlDao = new OlapDao();
    }

    @After
    public void tearDown() throws Exception {

    }

    public class PrettyPrintingMap<K, V> {
        private Map<K, V> map;

        public PrettyPrintingMap(Map<K, V> map) {
            this.map = map;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<K, V> entry = iter.next();
                sb.append(entry.getKey());
                sb.append('=').append('"');
                sb.append(entry.getValue());
                sb.append('"');
                if (iter.hasNext()) {
                    sb.append(',').append(' ');
                }
            }
            return sb.toString();
        }
    }

    @Test
    public void testExplain() throws Exception {
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) sqlDao
                .explain("select * from customer where nested('contracts', contracts.contract_id is not null) limit 3").explain();
        SearchHit[] hits = ((SearchResponse) select.get()).getHits().hits();
        Arrays.asList(hits).stream().forEach(x -> System.out.println(x.sourceAsString()));
        System.out.println(hits.length);
    }

    @Test
    public void testNestedFieldTest() throws Exception {
        String sql = "select avg(delegations.area_min) from customer where phone='15801378848' group by nested(delegations.biz_circle)"; // ok
        String sql2 = "";
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) sqlDao.explain("select count(*) from customer group by nested(contracts.district_name)").explain();
        InternalNested nested = ((SearchResponse) select.get()).getAggregations().get("contracts.district_name@NESTED");
        Terms infos = nested.getAggregations().get("contracts.district_name");
        for(Terms.Bucket bucket : infos.getBuckets()) {
            String key = bucket.getKey().toString();
            long count = bucket.getDocCount();
            System.out.println(String.format("key: %s, count: %d.", key, count));
        }
    }
}
