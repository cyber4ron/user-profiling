package com.lianjia.profiling.hbase

import com.lianjia.profiling.util.{DateUtil, ZipUtil}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat, HFileOutputFormat2}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object LoadHFile {
  val logger = LoggerFactory.getLogger(LoadHFile.getClass)

  def removePath(path: String) {
    val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUri), hadoopConf)

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

  /**
    * dump to hfile
    */
  def saveHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], tableName: String, path: String) {
    val outputConfig = {
      val conf = HBaseConfiguration.create()
      val job = Job.getInstance(conf, "profiling-dump-to-hbase")
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

  /**
    * dump to hbase table
    * https://hbase.apache.org/book.html#_bulk_load
    */
  def saveHFileAndLoadToTable(rdd: RDD[(ImmutableBytesWritable, KeyValue)], tableName: String, path: String)  {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val table = new HTable(conf, tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad (job, table)

    // Save HFiles on HDFS
    rdd.saveAsNewAPIHadoopFile(path,
                               classOf[ImmutableBytesWritable], classOf[KeyValue],
                               classOf[HFileOutputFormat], job.getConfiguration)

    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 256)

    // Bulk load HFiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(path), table)
  }

  def createTable(tableName: String) {
    LoadHFile.getClass.synchronized {
      if(!TableManager.exists(tableName)) TableManager.create(tableName, false)
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("profiling-dump-to-hbase")
    if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val date = conf.get("spark.backtrace.date")
    val month = DateUtil.getMonth(date)

    val eventTableName = s"${TableManager.ONLINE_USER_EVENT_PREFIX}$month"
    val preferTableName = "profiling:online_user_prefer_v3"

    val eventSortedName = s"/user/bigdata/profiling/online_user_event_sorted_$date"
    val eventHFileName = s"/user/bigdata/profiling/online_user_event_hfile_$date"
    val preferHFileName = s"/user/bigdata/profiling/online_user_prefer_hfile_$date"

    val evtPath = s"/user/bigdata/profiling/online_user_assoc_$date"
    val mergedIncPreferPath = s"/user/bigdata/profiling/online_user_prefer_inc_$date"

    logger.info("path: " + eventTableName)
    logger.info("path: " + preferTableName)
    logger.info("path: " + eventHFileName)
    logger.info("path: " + preferHFileName)
    logger.info("path: " + evtPath)
    logger.info("path: " + mergedIncPreferPath)

    // create table at start of month
    if(DateUtil.alignedByMonth() == new DateTime().minusDays(1).withTimeAtStartOfDay().toDate) {
      createTable(eventTableName)
      createTable(preferTableName)
    }

    // online user events
    val dumpUserEvents = sc.getConf.get("spark.backtrace.userEvents", "false").toBoolean

    if(dumpUserEvents) {
      val evtParts = if (date == DateUtil.getYesterday) 8 else 128
      logger.info("evtParts: " + evtParts)

      val userEventsSorted = sc.textFile(evtPath) flatMap { case line =>
        try {
          val Array(id, evtsBase64) = line.split("\t", 2)
          Array[(String, String)]((id, evtsBase64))
        } catch {
          case ex: Throwable =>
            logger.info("====> line: " + line)
            logger.info("====> ex: " + ex)
            Array.empty[(String, String)]
        }
      } sortByKey(true, evtParts)

      removePath(eventSortedName)
      userEventsSorted map { case (id, evtsBase64) => s"$id\t${new String(ZipUtil.deflate(evtsBase64.getBytes))}" } saveAsTextFile eventSortedName

      val userEventsSortedKV = userEventsSorted map { case (id, evtsBase64) =>
        (new ImmutableBytesWritable(id.getBytes()), new KeyValue(id.getBytes(),
                                                                 "evt".getBytes(),
                                                                 "".getBytes,
                                                                 evtsBase64.getBytes()))
      }

      removePath(eventHFileName)
      saveHFileAndLoadToTable(userEventsSortedKV, eventTableName, eventHFileName)
    }

    // online user preference, daily incremental
    val dumpUserPreference = sc.getConf.get("spark.backtrace.userPrefer", "false").toBoolean

    if(dumpUserPreference) {
      val prfParts = if (date == DateUtil.getYesterday) 4 else 16
      logger.info("prfParts: " + prfParts)

      val userPreferSorted = sc.textFile(mergedIncPreferPath) flatMap { case line =>
        try {
          val Array(id, prefer) = line.split("\t", 2)
          Array[(String, String)]((id, prefer))
        } catch {
          case ex: Throwable =>
            logger.info("====> line: " + line)
            logger.info("====> ex: " + ex)
            Array.empty[(String, String)]
        }
      } sortByKey(true, prfParts) map { case (id, evtsBase64) =>
        (new ImmutableBytesWritable(id.getBytes()), new KeyValue(id.getBytes(),
                                                                 "prf".getBytes(),
                                                                 "".getBytes, evtsBase64.getBytes()))
      }

      removePath(preferHFileName)
      // e.g. profiling:online_user_prefer_v2, /user/bigdata/profiling/xxx_0
      saveHFileAndLoadToTable(userPreferSorted, preferTableName, preferHFileName)
    }

    sc.stop()
  }
}
