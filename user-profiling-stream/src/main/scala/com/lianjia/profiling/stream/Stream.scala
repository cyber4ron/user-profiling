package com.lianjia.profiling.stream

import java.lang.Long

import com.lianjia.data.profiling.log.Logger
import com.lianjia.profiling.common.elasticsearch.Types
import com.lianjia.profiling.common.elasticsearch.index.IndexRoller
import com.lianjia.profiling.common.ha.StreamHAManager
import com.lianjia.profiling.common.hbase.client.BlockingBatchWriteHelper
import com.lianjia.profiling.common.hbase.roller.OnlineUserTableRoller
import com.lianjia.profiling.common.{BlockingBackoffRetryProxy, Logging}
import com.lianjia.profiling.config.Constants
import com.lianjia.profiling.stream.parser.OnlineUserMessageParser
import com.lianjia.profiling.util.{Arguments, ExUtil}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object Stream extends App with Logging {
  new Stream().parseArgs(args).asInstanceOf[Stream].start()

  def runKafkaVM(stream: InputDStream[(String, String)], conf: Map[String, String]): Unit = {

    var linesHouseEval = 0
    var linesElse = 0

    // var offsetRanges = Array.empty[OffsetRange]

    stream transform { rdd =>
      // offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    } map (_._2) foreachRDD { rdd =>

      rdd.foreachPartition { lines =>

        val logger = getLogger(classOf[Stream].getName)
        val esProxy = new BlockingBackoffRetryProxy(conf)

        // logger.info(offsetRanges.mkString("\n"))

        lines foreach { line =>
          try {
            line match {
              case x if x contains "NOTICE:" =>
                linesHouseEval += 1
                LineHandler.processHouseEval(line, esProxy)

              case _ =>
                linesElse += 1
            }
          } catch {
            case ex: Exception =>
              // fail silently
              logger.warn(s"exception while processing line, ex: ${ExUtil.getStackTrace(ex)}, line: $line")
          }
        }

        // purge
        esProxy.flush()

        // log
        logger.info(s"lines of house eval: $linesHouseEval")
        logger.info(s"lines of others: $linesElse")

        // submit topic offsets
        // submitOffsets(offsetRanges, esProxy, logger)
      }
    }
  }

  def run(stream: InputDStream[(String, String)], conf: Map[String, String]) {
    val toES = java.lang.Boolean.parseBoolean(conf.getOrElse[String]("spark.backtrace.to.es", "false"))
    val toHBase = java.lang.Boolean.parseBoolean(conf.getOrElse[String]("spark.backtrace.to.hbase", "false"))

    // todo: roller一个executor执行一次就行
    val indexRoller = new IndexRoller(Constants.ONLINE_USER_IDX, conf.asJava)
    indexRoller.roll()

    if(toHBase) {
      val onlineUserTblRoller = new OnlineUserTableRoller(Constants.ONLINE_USER_TBL, conf.asJava)
      onlineUserTblRoller.roll()
    }

    var linesOnlineUser = 0
    var linesHouseEval = 0
    var linesElse = 0

    var offsetRanges = Array.empty[OffsetRange]

    stream transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // commented due to use "getStreamAlt" method
      rdd
    } map (_._2) foreachRDD { rdd =>
      rdd.foreachPartition { lines =>
        val logger = getLogger(classOf[Stream].getName)

        logger.info(s"toES: $toES")
        logger.info(s"toHBase: $toHBase")

        val indexListener = new IndexRoller(Constants.ONLINE_USER_IDX, conf.asJava)
        indexListener.listen()

        var hbaseProxy: BlockingBatchWriteHelper = null
        var hbaseIndexProxy: BlockingBatchWriteHelper = null

        if (toHBase) {
          val onlineUserTblListener = new OnlineUserTableRoller(Constants.ONLINE_USER_TBL, conf.asJava)
          onlineUserTblListener.listen()

          hbaseProxy = new BlockingBatchWriteHelper(onlineUserTblListener.getTable)
          hbaseIndexProxy = new BlockingBatchWriteHelper(onlineUserTblListener.getTableIdx)
        }
        val esProxy = new BlockingBackoffRetryProxy(conf)

        logger.info(offsetRanges.mkString("\n"))

        lines foreach { line =>
          try {
            line match {
              case x if x contains "[bigdata." =>
                linesOnlineUser += 1
                LineHandler.processOnlineUser(line, esProxy, hbaseProxy, hbaseIndexProxy, indexListener,
                                              OnlineUserMessageParser.parse, toES, toHBase)

              case x if x contains "NOTICE:" =>
                linesHouseEval += 1
                LineHandler.processHouseEval(line, esProxy)

              case _ =>
                linesElse += 1
            }
          } catch {
            case ex: Exception =>
              // fail silently
              logger.warn(s"exception while processing line, ex: ${ExUtil.getStackTrace(ex)}, line: $line")
          }
        }

        // purge
        if (toES) esProxy.flush()
        if (toHBase) hbaseProxy.flush()

        // log
        logger.info(s"lines of online user: $linesOnlineUser")
        logger.info(s"lines of house eval: $linesHouseEval")
        logger.info(s"lines of others: $linesElse")

        // submit topic offsets
        submitOffsets(offsetRanges, esProxy, logger)
      }
    }
  }

  def submitOffsets(offsetRanges: Array[OffsetRange], esProxy: BlockingBackoffRetryProxy, logger: Logger) {
    // 处理完一个micro batch, 把offsets存放在es中
    var resp: BulkResponse = null
    import Types.JMap
    do {
      val bulk = esProxy.getESClient.prepareBulk()
      offsetRanges foreach { range =>
        val doc = new JMap[String, Object]()
        doc.put("topic", range.topic)
        doc.put("partition", new Integer(range.partition))
        doc.put("offset", new Long(range.untilOffset))
        bulk.add(new UpdateRequest("stream_offset", range.topic, range.partition.toString).doc(doc).docAsUpsert(true))
      }
      try {
        resp = bulk.get()
      } catch {
        case _: Throwable =>
          Thread.sleep(10)
      }

      logger.info("stream offset committed.")

    } while (resp.hasFailures)
  }
}

class Stream extends Arguments with StreamBase {
  def start() {
    if (System.getProperty("os.name") != "Mac OS X") {
      val ha = new StreamHAManager()
      ha.pollingUntilBecomeMasterUnlessJobFinished()
    }

    val sc = getSc

    // sc.sparkContext.applicationId

    // https://hadoop.apache.org/docs/r2.4.1/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_API

    // finished-> exit
    // http://jx-bd-hadoop00.lianjia.com:8088/ws/v1/cluster/apps/application_1463404263028_101720
    //{
    // "app": {
    //   ...
    //   "state": "FINISHED",
    //   "finalStatus": "SUCCEEDED",
    //   ...
    // }

    // 正常
    // http://jx-bd-hadoop00.lianjia.com:8088/ws/v1/cluster/apps/application_1463404263028_113864
    //
    //  "app": {
    //    ...
    //    "state": "RUNNING",
    //    "finalStatus": "UNDEFINED",
    //    ...

    sc.sparkContext.addSparkListener(new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        System.err.println(s"job ended, job id: ${jobEnd.jobId}, result: ${jobEnd.jobResult}.")
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd)  { // todo: test
        if (sc.sparkContext.isStopped) {
          System.err.println("stopping spark streaming context...")
          sc.stop(stopSparkContext = true, stopGracefully = true)
        }
      }
    })

    val stream = getStream(sc)
    //val kafkaVMStream = getKafkaVMStream(sc)
    val conf = sc.sparkContext.getConf.getAll.toMap

    Stream.run(stream, conf)
    //Stream.runKafkaVM(kafkaVMStream, conf)

    sc.checkpoint("profiling/steaming-checkpoints")
    sc.start()
    sc.awaitTermination()

    sc.stop()
  }
}
