package com.lianjia.profiling.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object UpdateTest extends App {

  val conf = new SparkConf().setMaster("local[1]").setAppName("es-spark-test")
  conf.set("es.nodes", "172.30.17.2:9200")
  // conf.set("spark.es.resource", "test/test")
  conf.set("es.mapping.id", "id")
  conf.set("spark.es.write.operation", "upsert")

  val sc = new SparkContext(conf)

  var users = Seq.empty[Map[String, AnyRef]]
  for (i <- 0 to 1 * 1000) users :+= Map("id" -> "001",
                                         "page_visits" -> Seq(Map("visit" -> "x99x00", "visit2" -> "x99x0011"))) // nested idxType

  sc.makeRDD(users).saveToEs("test/test")
}
