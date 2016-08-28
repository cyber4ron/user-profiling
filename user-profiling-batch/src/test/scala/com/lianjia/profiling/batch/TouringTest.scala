package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object TouringTest extends App {
  val proxy = new BlockingBackoffRetryProxy(new SparkConf().set("spark.es.cluster.name", "profiling")
                                            .set("spark.es.cluster.nodes", "10.10.35.14:9300")
                                            .getAll.toMap)
  val req = RowParser.parseTouring("2215801378848\t130470580\t501161238678\t1000000020073228\t20073225\t2014-06-11 00:00:00\t2014-06-11 00:00:00\t也主不组我的客户，想租学生\t200200000002\t100\t120000")
  req.foreach(proxy.send)
  proxy.flush()

  // Thread.sleep(1000 * 1000)
}
