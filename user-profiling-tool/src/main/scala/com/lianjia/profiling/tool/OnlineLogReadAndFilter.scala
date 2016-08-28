package com.lianjia.profiling.tool

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object OnlineLogReadAndFilter extends App {
  val conf = new SparkConf().setAppName("profiling-online-log-read-and-filter")
  val sc = new SparkContext(conf)

  sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
  sc.textFile(s"/user/hive/warehouse/log_center.db/dim_save_log_detail_pc/pt=20160516*")
  .filter(_.contains("BJSJ92018676"))
  .saveAsTextFile("/user/bigdata/profiling/filtered")

  def getInc(): Unit = {
//    val inc = Batch.getInc(sc, hiveCtx.read.table("profiling.touring_house20160521").map(_.mkString("\t")),
//                     hiveCtx.read.table("profiling.touring_house20160520").map(_.mkString("\t")),
//                     "20160521", "20160520")
  }

  sc.stop()
}
