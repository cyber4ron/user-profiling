package com.lianjia.profiling.hbase

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.lianjia.profiling.util.DateUtil
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object LoadHFileTest extends App {
  val conf = new SparkConf().setAppName("profiling-load-to-hbase")
  val sc = new SparkContext(conf)

  val yesterday = DateUtil.getYesterday

  val user_evts = sc.textFile(s"/user/bigdata/profiling/assoc_$yesterday") flatMap { case line =>
    try {
      val Array(id, evts) = line.split("\t", 2)
      Array[(String, String)]((id, evts))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } sortByKey() /*map { case (id, evts) =>
    (new ImmutableBytesWritable(id.getBytes()), new KeyValue(id.getBytes(), "evt".getBytes(), "".getBytes, evts.getBytes()))
  }*/



  sc.textFile("/user/bigdata/profiling/dfdsdf899331") flatMap { case line =>
    try {
      val Array(id, evts) = line.split("\t", 2)
      Array[(String, String)]((id, ""))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } sortByKey() coalesce(32, false) saveAsTextFile "/user/bigdata/profiling/test333990eeeeeerrq9_tmp"

  sc.textFile("/user/bigdata/profiling/dfdsdf899331") flatMap { case line =>
    try {
      val Array(id, evts) = line.split("\t", 2)
      Array[(String, String)]((id, deflate(evts.getBytes)))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } sortByKey() map { case (id, evts) =>
    (new ImmutableBytesWritable(id.getBytes()), new KeyValue(id.getBytes(), "evt".getBytes(), "".getBytes, evts.getBytes()))
  } saveAsTextFile "/user/bigdata/profiling/test333990eeeeeerrq21_tmp"

  def deflate(bytes: Array[Byte]): String = {
    val arrOutputStream = new ByteArrayOutputStream()
    val zipOutputStream = new GZIPOutputStream(arrOutputStream)
    zipOutputStream.write(bytes)
    zipOutputStream.close()
    Base64.encodeBase64String(arrOutputStream.toByteArray)
  }

  def inflate(base64: String): String = {
    val bytes = Base64.decodeBase64(base64)
    val zipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val buf: Array[Byte] = new Array[Byte](2048)
    var num = -1
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    while ( { num = zipInputStream.read(buf, 0, buf.length); num != -1 }) {
      byteArrayOutputStream.write(buf, 0, num)
    }
    val inflated = byteArrayOutputStream.toString
    zipInputStream.close()
    byteArrayOutputStream.close()
    inflated
  }

  sc.textFile("/user/bigdata/profiling/dfdsdf899331") flatMap { case line =>
    try {
      val Array(id, evts) = line.split("\t", 2)
      Array[(String, String)]((id, deflate(evts.getBytes)))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } sortByKey() saveAsTextFile "/user/bigdata/profiling/test333990eeeeeerrq21_tmp" // ok. 1900个partition

  val userEventsSorted = sc.textFile("/user/bigdata/profiling/test333990eeeeeerrq21_tmp") flatMap { case line =>
    try {
      val Array(id, evtsBase64) = line.substring(1, line.length - 1).split(",", 2)
      Array[(String, String)]((id, inflate(evtsBase64)))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } coalesce (32, false) saveAsTextFile "/user/bigdata/profiling/test333990eeeeeerrq21_tmp_inflate" // 可以执行完, 但是会乱序..

  // val ordering = implicitly[Ordering[(String, String)]]
  val userEventsSortedCoalesced = sc.textFile("/user/bigdata/profiling/test333990eeeeeerrq21_tmp") flatMap { case line =>
    try {
      val Array(id, evtsBase64) = line.substring(1, line.length - 1).split(",", 2)
      Array[(String, String)]((id, evtsBase64))
    } catch {
      case ex: Throwable =>
        System.err.println("====> line: " + line)
        System.err.println("====> ex: " + ex)
        Array.empty[(String, String)]
    }
  } sortByKey (true, 32) map { case (id, evtsBase64) =>
    (new ImmutableBytesWritable(id.getBytes()), new KeyValue(id.getBytes(),
                                                             "evt".getBytes(),
                                                             "".getBytes, evtsBase64.getBytes()))
  }
}
