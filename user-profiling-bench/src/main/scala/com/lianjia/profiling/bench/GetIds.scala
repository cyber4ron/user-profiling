package com.lianjia.profiling.bench

import com.alibaba.fastjson.{JSONObject, JSON}
import org.apache.spark.{SparkContext, SparkConf}

import org.elasticsearch.spark._

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object GetIds extends App {

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"

  val conf = new SparkConf().setAppName("bench")/*.setMaster("local[1]")*/
  val sc = new SparkContext(conf)

  sc.esJsonRDD("online_user_20160620/") map {
    case (_, json) =>
      val obj = JSON.parse(json).asInstanceOf[JSONObject]
  }

  sc.esJsonRDD("online_user_20160620/") map {
    case (_, json) => val obj = JSON.parse(json).asInstanceOf[JSONObject]
      if (java.lang.Long.parseLong(obj.get("ts").toString) >= 1466697600000L &&
        java.lang.Long.parseLong(obj.get("ts").toString) <= 1466784000000L)
        json
      else ""
  } filter (_ != "") saveAsTextFile s"$hdfsUri/user/bigdata/profiling/0624"

  sc.esJsonRDD("online_user_20160620") map {
    case (_, json) => val obj = JSON.parse(json).asInstanceOf[JSONObject]
      if (java.lang.Long.parseLong(obj.get("ts").toString) >= 1466697600000L)
        obj.get("ucid").toString
      else ""
  } filter (_ != "") saveAsTextFile s"$hdfsUri/user/bigdata/profiling/bench/ucid0626"

  sc.esJsonRDD("customer/customer") map {
    case (phone, _) => s"$phone"
  } saveAsTextFile s"$hdfsUri/user/bigdata/profiling/bench/phone"

  sc.stop()
}
