package com.lianjia.profiling.stream

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

// import org.elasticsearch.spark._

object ParseJsonTest {
  def main(args: Array[String]) {
    // for (line <- Source.fromFile("kafkaMessage").getLines()) parseKafkaMessage(line)

    writeToES(getSc)

    //    println(ESFieldUtil.toDateTime("20150301151601"))
    //    println(ESFieldUtil.toDate("20150301151601"))
  }

  def getSc: SparkContext = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("es-spark-test")
    conf.set("es.nodes", "172.30.17.2:9200")
    conf.set("spark.es.resource", "delegation2/delegation")

    new SparkContext(conf)
  }

  def writeToES(sc: SparkContext): Unit = {
    //    val dels = Seq(Map("del_id" -> "d001", "detail" -> "dtl001"),
    //                   Map("del_id" -> "d002", "detail" -> "dtl002"))
    //    val doc1 = Map("cust_id" -> "c01", "delegations" -> dels)
    //    val doc2 = Map("cust_id" -> "c02", "delegations" -> dels)
    //
    //    sc.makeRDD(Seq(doc1, doc2)).saveToEs("test/nested_type")

    //    Lat Lon as Stringedit
    //
    //    Format in lat,lon.
    //
    //    {
    //      "pin" : {
    //        "location" : "41.12,-71.34"
    //      }
    //    }

    val ts = System.currentTimeMillis()
    println(System.currentTimeMillis())

    val mobile_pvs = Seq(Map("timestamp" -> "2013-07-05 08:49:30.123",
                             "location" -> "41.12,-71.34",
                             "city_id" -> "110000"))


    var users = Seq.empty[Map[String, AnyRef]]
    for (i <- 0 to 1 * 1000) users :+= Map("uuid" -> s"u00$i",
                                           "ucid" -> "uc001",
                                           "mobile_page_visits" -> mobile_pvs)

    //nested ts kibana似乎不识别
    sc.makeRDD(users).saveToEs("user/user")

    // vm2 es test:
    // 10000本地3.4s - 0.34ms avg
    // 10w本地60s, 43s
    // batch size的缘故? todo

    println(System.currentTimeMillis() - ts)
  }

}
