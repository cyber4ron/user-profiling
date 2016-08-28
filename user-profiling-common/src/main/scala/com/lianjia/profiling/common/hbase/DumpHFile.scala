package com.lianjia.profiling.common.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._


object DumpHFile {

  def convertScanToString(scan: Scan) = {
    val proto : ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  val sc = new SparkContext(new SparkConf())

  val family = Bytes.toBytes("cf")

  val inputRDD = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "input_table")
    conf.set(TableInputFormat.SCAN, convertScanToString(new Scan()))
    sc.newAPIHadoopRDD(conf,
                       classOf[TableInputFormat],
                       classOf[ImmutableBytesWritable],
                       classOf[Result])
  }

  val outputConf = {
    val conf = HBaseConfiguration.create()
    val job = Job.getInstance(conf, "DumpHFile")
    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])

    val conn = ConnectionFactory.createConnection(conf)
    val tblName = TableName.valueOf("tbl1")
    HFileOutputFormat2.configureIncrementalLoad(job,
                                                conn.getTable(tblName).getTableDescriptor,
                                                conn.getRegionLocator(tblName))
    conf
  }

  def saveHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], tableName: String, path: String) {
    val outputConfig = {
      val conf = HBaseConfiguration.create()
      val job = Job.getInstance(conf, "DumpHFile")
      job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass (classOf[KeyValue])

      val conn = ConnectionFactory.createConnection(conf)
      val tblName = TableName.valueOf(tableName)
      HFileOutputFormat2.configureIncrementalLoad(job,
                                                  conn.getTable(tblName).getTableDescriptor,
                                                  conn.getRegionLocator(tblName))
      conf
    }

    rdd.saveAsNewAPIHadoopFile(path,
                               classOf[ImmutableBytesWritable],
                               classOf[Put],
                               classOf[HFileOutputFormat2],
                               outputConfig)
  }

  def main(args: Array[String]) {
    inputRDD.flatMap { kv =>
      kv._2.getFamilyMap(family).entrySet().iterator().map { pairs =>
        (kv._1, new KeyValue(kv._1.copyBytes(), family, pairs.getKey, pairs.getValue))
      }
    }.saveAsNewAPIHadoopFile("hdfs://localhost.localdomain:8020/user/cloudera/spark",
                             classOf[ImmutableBytesWritable],
                             classOf[Put],
                             classOf[HFileOutputFormat2],
                             outputConf)
  }
}
