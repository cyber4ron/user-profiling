package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object DelegationTest extends App {
  val map = new SparkConf().set("spark.es.cluster.name", "profiling").set("spark.es.cluster.nodes", "10.10.35.14:9300").getAll.toMap
  val proxy = new BlockingBackoffRetryProxy(map)
  val req = RowParser.parseDel("2213752356299\t501154393987\tc+阿姨2\t200400000008\t2012-12-26\t2013-01-12\t2014-10-07 00:00:00\t1000000020023120\t20023120\t0\tNULL\tNULL\t200200000001\t120000\t100\tNULL\t南开\t学府街\t1\t1\t50.0\t60.0\t600000.0\t750000.0\t0\t0")
  req.foreach(proxy.send)
  proxy.flush()
  Thread.sleep(1000 * 1000)
}
