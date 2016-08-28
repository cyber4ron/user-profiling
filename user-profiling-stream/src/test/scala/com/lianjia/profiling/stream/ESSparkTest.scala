package com.lianjia.profiling.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object ESSparkTest extends App {

  // ?? -DHADOOP_USER_NAME=bigdata
  val conf = new SparkConf().setMaster("local[5]").setAppName("es-spark-test")
  conf.set("es.nodes", "172.30.17.2:9200")
  conf.set("spark.es.resource", "delegation2/delegation")

  val sc = new SparkContext(conf)

  val rdd = sc.esRDD("delegation2/delegation")

  println(rdd.count())

  println("x")

  def write(): Unit = {

  }
}
