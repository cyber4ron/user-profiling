package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf
import org.elasticsearch.action.search.{SearchAction, SearchRequestBuilder}
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object DeleteByQuery extends App {
  val map = new SparkConf().set("spark.es.cluster.name", "profiling")
            .set("spark.es.cluster.nodes", "10.10.35.14:9300").getAll.toMap
  val proxy = new BlockingBackoffRetryProxy(map)

  val qb: QueryBuilder = QueryBuilders.rangeQuery("write_ts")
                         .from("20160603T150000+0000")
                         .to("20160603T210000+0000")

  val x = new SearchRequestBuilder(proxy.getESClient, SearchAction.INSTANCE)
          .setIndices("customer_touring")
          .setTypes("touring")
          .setQuery(qb)
          .get()

  val cnt = x.getHits

  println(cnt)

  //  val rsp = new DeleteByQueryRequestBuilder(proxy.getESClient, DeleteByQueryAction.INSTANCE)
  //            .setIndices("customer_touring")
  //            .setTypes("touring")
  //            .setQuery(qb)
  //            .get()

  //  val cnt = rsp.getTotalDeleted
//  println(cnt)
}
