package com.lianjia.profiling.stream.backtrace

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.common.elasticsearch.Types.JMap
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-05
  */
object HouseEvalLogBacktrace extends App {
  val conf = new SparkConf().setAppName("HouseEvalLogBacktrace")
  val sc = new SparkContext(conf)
  val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
  val data = hiveCtx.sql("select user_input from house_price_prediction.prediction_info")

  run(conf.getAll.toMap) // put code in run(). App is delayed initialized

  sc.stop()

  def run(conf: Map[String, String]) {
    data foreachPartition { partition =>
      import scala.collection.JavaConversions._
      val esProxy = new BlockingBackoffRetryProxy(conf)

      var docs = 0
      var lines = 0

      for (line <- partition) {
        lines += 1

        val user_input = line.getAs[String]("user_input")
        val feats = JSON.parseObject(user_input)

        val doc = new JMap[String, AnyRef]()
        for (ft <- feats.entrySet()) doc.put(ft.getKey, ft.getValue)

        if (feats.containsKey("request_id")) {
          val req = new UpdateRequest("house_eval", "ft", feats.get("request_id").toString).doc(doc).docAsUpsert(true)
          esProxy.send(req)
          docs += 1
        }

        if (docs % 10000 == 0) System.err.println(s"$docs doc sent.")
        if (lines % 10000 == 0) System.err.println(s"$lines doc sent.")
      }

      esProxy.flush()

      System.err.println(s"$docs doc sent.")
      System.err.println(s"$lines doc sent.")
    }
  }
}
