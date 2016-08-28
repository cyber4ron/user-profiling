package com.lianjia.profiling.batch

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object HouseTest extends App {
  val map = new SparkConf().set("spark.es.cluster.name", "profiling").set("spark.es.cluster.nodes", "10.10.35.14:9300").getAll.toMap
  val proxy = new BlockingBackoffRetryProxy(map)
  val req = RowParser.parseHouse("aaBJHD87029132\t1115029404223\t1000000020073556\t20073556\t200100000009\t2014-01-15 13:37:12\t2014-02-23 20:31:36\t0000-00-00 00:00:00\t200200000002\t4800.0\t200300000003\t2\t1\t1\t1\t0\t53.4\t23008618\t海淀区\t611100529\t万寿路\t1111027376590\t翠微路22号院\t1112027424663\t北1号楼（原1号楼）\t1113027493904\t1单元\t1114028745547\t5\t107500000003\t普通住宅\t6\t1980\tNULL\t500400000500\t2500米以内\tNULL\t112100000004\t精装\t110000\t北京\t1\t200")
  req.foreach(proxy.send)
  proxy.flush()
  Thread.sleep(1000 * 1000)
}
