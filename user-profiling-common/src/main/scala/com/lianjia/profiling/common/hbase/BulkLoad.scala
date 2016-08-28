package com.lianjia.profiling.batch.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext


/**
  * @author fenglei@lianjia.com on 2016-04
  */

class BulkLoad {

  val tableName = "t1"
  val columnFamily1 = "f1"
  val columnFamily2 = "f2"

  val envMap = Map[String,String](("Xmx", "512m"))
  val sc = new SparkContext("local", "test", null, Nil, envMap)


  val rdd = sc.parallelize(Array(
    (Bytes.toBytes("1"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1"))),
    (Bytes.toBytes("3"),
      (Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo2.a"))),
    (Bytes.toBytes("3"),
      (Bytes.toBytes(columnFamily2), Bytes.toBytes("a"), Bytes.toBytes("foo2.b"))),
    (Bytes.toBytes("3"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.c"))),
    (Bytes.toBytes("5"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3"))),
    (Bytes.toBytes("4"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1"))),
    (Bytes.toBytes("4"),
      (Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo.2"))),
    (Bytes.toBytes("2"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1"))),
    (Bytes.toBytes("2"),
      (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("bar.2")))))


  val config  = new Configuration()

}
