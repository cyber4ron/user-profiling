package com.lianjia.profiling.stream.backtrace

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.common.elasticsearch.index.IndexRoller
import com.lianjia.profiling.common.ha.CustomHAManager
import com.lianjia.profiling.common.hbase.client.BlockingBatchWriteHelper
import com.lianjia.profiling.common.hbase.roller.OnlineUserTableRoller
import com.lianjia.profiling.common.redis.PipelinedJedisClient
import com.lianjia.profiling.config.Constants
import com.lianjia.profiling.stream.LineHandler
import com.lianjia.profiling.stream.Stream._
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder
import com.lianjia.profiling.stream.parser.FlumeOnlineUserMessageParser
import com.lianjia.profiling.util.{DateUtil, ExUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Days
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object OnlineUserBacktrace {
  val logger = LoggerFactory.getLogger(OnlineUserBacktrace.getClass)

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUri), hadoopConf)

  def removePath(path: String) {
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

  def run(args: Array[String]) {
    val sc = getSc
    val conf = sc.getConf

    // index最好提前建好, 设置下refresh interval和replica num
    val paths = if (sc.getConf.contains("spark.backtrace.paths")) sc.getConf.get("spark.backtrace.paths").split(",")
    else Array("/user/hive/warehouse/log_center.db/dim_save_log_event_mobile/pt=",
               "/user/hive/warehouse/log_center.db/dim_save_log_detail_pc/pt=",
               "/user/hive/warehouse/log_center.db/dim_save_log_follow_pc/pt=",
               "/user/hive/warehouse/log_center.db/dim_save_log_search_pc/pt=")

    val startDateStr = sc.getConf.get("spark.backtrace.onlineUser.startDate")
    val start = DateUtil.parseDate(startDateStr)
    val end = DateUtil.parseDate(sc.getConf.get("spark.backtrace.onlineUser.endDate")) // inclusive

    var ha: CustomHAManager = null
    if (start == end) {
      ha = new CustomHAManager("/profiling/stream/daily-batch/online-user/master",
                               "/profiling/stream/daily-batch/online-user/job/",
                               startDateStr)
      ha.pollingUntilBecomeMasterUnlessJobFinished()
    }

    @volatile var isFailed = false
    sc.addSparkListener(new SparkListener { // todo: test
      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        System.err.print(s"job ended, job id: ${jobEnd.jobId}, result: ${jobEnd.jobResult}.")
        jobEnd.jobResult match {
          case JobSucceeded =>
          case _ => isFailed = true
        }
      }
    })

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    import concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val daysCount = Days.daysBetween(start, end).getDays
    var tasks = List.empty[Future[Unit]]
    (0 to daysCount).map(start.plusDays).map(DateUtil.toFormattedDate).foreach { date =>
      clearAndCreateIdxAndTbl(date, conf.getAll.toMap)
      tasks :+= Future {
        val pathsStr = paths.map(path => s"$path$date*").mkString(",")
        System.err.println(s"backtracing $date, path: $pathsStr")
        backtraceAndDumpToHdfs(sc.textFile(pathsStr), date, conf.getAll.toMap)
      }
    }

    Await.result(Future.sequence(tasks), 24.hours)

    if (!isFailed) ha.markJobAsFinished()

    sc.stop()
  }

  def getSc = {
    if (System.getProperty("os.name") == "Mac OS X") {
      val conf = new SparkConf().setMaster("local[1]").setAppName("profiling-online-user-backtrace")
      new SparkContext(conf)
    }
    else {
      val conf = new SparkConf().setAppName("profiling-online-user-backtrace")
      new SparkContext(conf)
    }
  }

  def clearAndCreateIdxAndTbl(date: String, conf: Map[String, String]) {
    val indexRoller = new IndexRoller(date, Constants.ONLINE_USER_IDX, conf.asJava)
    indexRoller.roll()
    val tblRoller = new OnlineUserTableRoller(date, "profiling:online_user", conf.asJava)
    tblRoller.roll()
  }

  def backtraceAndDumpToHdfs(data: RDD[String], date: String, conf: Map[String, String]) {
    var linesOnlineUser = 0
    var linesElse = 0
    val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"

    val dumpPath = s"$hdfsUri/user/bigdata/profiling/online_events_$date"
    removePath(dumpPath)

    data mapPartitions { partition =>
      println("new partition...")
      val indexListener = new IndexRoller(date, Constants.ONLINE_USER_IDX, conf.asJava)
      indexListener.listen()
      val onlineUserTblListener = new OnlineUserTableRoller(date, "profiling:online_user", conf.asJava)
      onlineUserTblListener.listen()

      val logger = getLogger(OnlineUserBacktrace.getClass.getName)
      val esProxy = new BlockingBackoffRetryProxy(conf)
      val hbaseProxy = new BlockingBatchWriteHelper(onlineUserTblListener.getTable)
      val hbaseIndexProxy = new BlockingBatchWriteHelper(onlineUserTblListener.getTableIdx)
      val redisClient = new PipelinedJedisClient

      val toES = java.lang.Boolean.parseBoolean(conf.getOrElse[String]("spark.backtrace.to.es", "false"))
      val toHBase = java.lang.Boolean.parseBoolean(conf.getOrElse[String]("spark.backtrace.to.hbase", "false"))
      val toRedis = java.lang.Boolean.parseBoolean(conf.getOrElse[String]("spark.backtrace.to.redis", "false"))

      val docs = new util.ArrayList[OnlineUserEventBuilder.Doc]()
      partition foreach { line =>
        try {
          line match {
            case x if x contains "[bigdata." => // 应该都有这个tag
              linesOnlineUser += 1
              docs.addAll(LineHandler.processOnlineUser(line,
                                                        esProxy, hbaseProxy, hbaseIndexProxy, redisClient,
                                                        indexListener, FlumeOnlineUserMessageParser.parse,
                                                        toES, toHBase))
            case _ =>
              linesElse += 1
          }
        } catch {
          case ex: Exception =>
            logger.warn(s"Exception while processing line, ex: ${ExUtil.getStackTrace(ex)}")
        }
      }

      // purge
      if (toES) hbaseProxy.flush()
      if (toHBase) esProxy.flush()
      if (toRedis) redisClient.flush()

      // log
      logger.info(s"lines of online user: $linesOnlineUser")
      logger.info(s"lines of others: $linesElse")

      import scala.collection.JavaConversions._

      { for (doc: OnlineUserEventBuilder.Doc <- docs)
          yield s"${doc.idxType}\t${JSON.toJSONString(doc.doc, SerializerFeature.QuoteFieldNames)}"
      }.iterator

    } coalesce (256, false) saveAsTextFile dumpPath
  }

  def main(args: Array[String]) {
    run(args)
  }
}
