package com.lianjia.profiling.bench

import com.lianjia.profiling.common.hbase.client.HBaseClient
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fenglei@lianjia.com on 2016-06
  */

/**
  * http://blog.csdn.net/liuxingjiaofu/article/details/7056966
  */
object HBaseBench extends App {

  val confx = HBaseConfiguration.create()
  val scannerTimeout = confx.getLong(
    HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1)

  confx.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000)

  println(System.currentTimeMillis())
  var client = new HBaseClient("profiling:online_user_201606")
  // val row = client.get("2000000007267101".getBytes)

  //  scan 'profiling:online_user_201606', {FILTER => "PrefixFilter('2000000007267101')"}
  // var events = client.filterByPrefix("2000000007267101".getBytes) // 1min...
  // hbase shell的话, 176 row(s) in 45.6950 seconds

  // val events2 = client.filterByRange("2000000007267101-1465770167".getBytes, "2000000007267101-1466325319".getBytes) // 慢...

  //scan 'profiling:online_user_201606', {STARTROW => "2000000007267101-0", ENDROW => "2000000007267101-2"}
  val events3 = client.scan("2000000007267101-0".getBytes, "2000000007267101-2".getBytes)

  println(System.currentTimeMillis())

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"

  val conf = new SparkConf().setAppName("bench").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val data = sc.textFile(s"$hdfsUri/user/bigdata/profiling/bench/ucid", 17).take(50000)

  val startMs = System.currentTimeMillis()

  // val count = data.count()

  def ucidSearch() = {
    sc.makeRDD(data) mapPartitions { part =>
      val client = new HBaseClient("profiling:online_user_201606")
      var before: Long = 0
      part map { ucid =>
        before = System.currentTimeMillis()
        val evts = client.scan(s"$ucid-0".getBytes, s"$ucid-2".getBytes)

        (System.currentTimeMillis() - before, 0)
      }
    }
  }

  var bench = ucidSearch() collect()
  var delays = bench map (_._1) sorted
  var delays2 = bench sortBy (_._1)
  var sizes = bench map (_._2) sorted
  val len = delays.length

  sc.stop()
}
