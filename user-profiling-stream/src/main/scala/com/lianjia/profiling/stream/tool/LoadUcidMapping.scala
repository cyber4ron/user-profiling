package com.lianjia.profiling.stream.tool

import java.util

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object LoadUcidMapping extends App {

  def processLine(line: String, esProxy: BlockingBackoffRetryProxy): Unit = {
    val parts = line.toString.split('\t')
    val ucid = parts(0)
    val phone = parts(5)
    if(ucid == "\\N" || phone == "\\N") return

    val doc = new util.HashMap[String, Object]()
    doc.put("ucid", ucid)
    doc.put("phone", phone)
    val req = new UpdateRequest("ucid_phone", "ucid_phone", phone).doc(doc).docAsUpsert(true)
    esProxy.send(req)
  }

  def run(path: String, sc: SparkContext, conf: Map[String, String]): Unit = {
    sc.textFile(path) foreachPartition { partition =>
      if(conf == null) println("====> conf == null")
      println(conf)
      println(conf.toString())
      val esProxy = new BlockingBackoffRetryProxy(conf)
      var count = 0
      for (line <- partition) {
        processLine(line, esProxy)
        count += 1
        if(count % 10000 == 0) println(s"$count doc sent.")
      }
      esProxy.flush()
    }

    sc.stop()
  }

  val date = args.toList match {
    case  ("--date" | "-d") :: value :: tail =>
      value
    case _ =>
      System.err.print("--date")
      sys.exit(1)
  }

  val sc = new SparkContext(new SparkConf().setAppName("UpdateUcidMapping"))
  val path = s"hdfs://jx-bd-hadoop00.lianjia.com:9000/user/hive/warehouse/data_center.db/dim_uc_user_day/pt=${date}000000/uc-v_user-${date}000000.txt"

  println(path)

  val conf = sc.getConf.getAll.toMap
  println(conf.toString())

  run(path, sc, conf)
}
