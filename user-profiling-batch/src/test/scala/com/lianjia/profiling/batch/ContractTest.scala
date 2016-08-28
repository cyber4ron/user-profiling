package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object ContractTest extends App {
  val map = new SparkConf().set("spark.es.cluster.name", "profiling").set("spark.es.cluster.nodes", "10.10.35.14:9300").getAll.toMap
  val proxy = new BlockingBackoffRetryProxy(map)
  val req = RowParser.parseContract("2218699008781\t100000012619\t501169023416\t101089355692\tNULL\t200800000003\t2015-12-15 11:46:10\t885000.0\t1000000020121252\t20121252\t2015-12-12 17:47:03\t200200000001\t120000\t100\t120111\t西青\t611100299\t中北镇\t1211045768495\t碧湖园\t2\t74.78\t6\t6\t0\tNULL\tNULL\tNULL\tNULL\tNULL\t0\t0\t900000.0")
  req.foreach(proxy.send)
  proxy.flush()
  Thread.sleep(1000 * 1000)
}
