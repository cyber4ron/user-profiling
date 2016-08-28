package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object TouringHouseTest extends App {
  val proxy = new BlockingBackoffRetryProxy(new SparkConf().set("spark.es.cluster.name", "profiling")
                                            .set("spark.es.cluster.nodes", "10.10.35.14:9300")
                                            .getAll.toMap)
  val req = RowParser.parseTouringHouse("2215801378848\t130470581\t164467741\t1000000020046088\t20046088\t2013-07-06 00:00:00\t120000\t101085269945\t1215050698679\t200200000001\t200600000001\t200700000002\t120102\t河东\t613000793\t春华街\t1211045473953\t汇和家园\t3\t129.0\t6\t6\t0\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\t0\t1600000.0")
  req.foreach(proxy.send)
  proxy.flush()

  // System.in.read()
}
