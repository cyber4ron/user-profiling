package com.lianjia.profiling.tool

import com.lianjia.profiling.util.DateUtil
import org.apache.hadoop.fs.Path
import org.elasticsearch.spark._

/**
  * @author fenglei@lianjia.com on 2016-05
  */

import org.apache.spark.{SparkConf, SparkContext}

object DailyDump extends App {
  val conf = new SparkConf().setAppName("profiling-index-daily-dump")
  val sc = new SparkContext(conf)

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUri), hadoopConf)
  val date = args.toList match {
    case "--date" :: value :: tail =>  value
    case _ => DateUtil.dateDiff(DateUtil.getDate(), -1)
  }

  def samplePhones() {
    val data = sc.esRDD("ucid_phone/ucid_phone")
    val dfsPath = "/user/bigdata/profiling/phones"
    data.sample(withReplacement = false, 1.0 / 900, seed = System.currentTimeMillis())
    .filter(x => x._2.contains("phone"))
    .map(x => x._2.get("phone").get)
    .saveAsTextFile(dfsPath)
  }

  def touchFile(filePath: String) {
    println(s"touring $filePath")
    hdfs.create(new Path(filePath), true)
  }

  def mkdirIfAbsent(path: String) {
    println(s"creating dir $path")
    hdfs.mkdirs(new Path(path))
  }

  def removePath(path: String) {
    try {
      println(s"removing $path")
      if(path.length < "/user/bigdata/profiling".length) {
        println(s"$path is to short!")
        return
      }
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch {
      case _: Throwable =>
        println(s"remove $path failed.")
    }
  }

  def saveToHDFS(esResource: String, path: String) {
    removePath(path)
    sc.esJsonRDD(esResource) map {
      case (id, src) => s"$id\t$src"
    } saveAsTextFile path
  }

  def rollingDelete() {
    var dateToDel = DateUtil.dateDiff(date, -7)
    removePath(s"/user/bigdata/profiling/customer_$dateToDel")
    removePath(s"/user/bigdata/profiling/house_$dateToDel")

    // online
    dateToDel = DateUtil.dateDiff(date, -7 * 24)
    val week = DateUtil.alignedByWeek(dateToDel)
    removePath(s"/user/bigdata/profiling/online_user_$week")
  }

  def dumpToHdfs() {
    Map("customer/customer" -> s"/user/bigdata/profiling/customer_$date",
        "house/house" -> s"/user/bigdata/profiling/house_$date") foreach {
      case (res, path) =>
        saveToHDFS(res, path)
        touchFile(s"$path/_success")
    }

    val week = DateUtil.alignedByWeek(date)
    val dir = "/user/bigdata/profiling"
    val idx = s"online_user_$week"
    mkdirIfAbsent(Array(dir, idx).mkString("/"))

    Array("dtl", "srh", "fl", "mob_dtl", "mob_srh", "mob_fl", "mob_clk") foreach { tp =>
        val dfsPath = Array(dir, idx, s"${tp}_$date").mkString("/")
        saveToHDFS(Array(idx, tp).mkString("/"), dfsPath)
        touchFile(s"${Array(dfsPath, "_SUCCESS").mkString("/")}")
    }
  }

  rollingDelete()
  dumpToHdfs()

  sc.stop()
}
