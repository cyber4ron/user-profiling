package com.lianjia.profiling.batch

import com.lianjia.data.profiling.log.LoggerFactory
import com.lianjia.profiling.batch.hive.HqlFactory
import com.lianjia.profiling.batch.spark.BatchBase
import com.lianjia.profiling.common.elasticsearch.index.IndexManager
import com.lianjia.profiling.common.ha.BatchHAManager
import com.lianjia.profiling.common.redis.PipelinedJedisClient
import com.lianjia.profiling.common.{BlockingBackoffRetryProxy, Logging}
import com.lianjia.profiling.util.{SMSUtil, DateUtil, ExUtil, Properties}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object Batch extends Logging {
  val batchTimeOutHr = 24
  val indexReplicas = 0
  val IndexRefreshInterval = "1s"
  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val hiveDbPath = "/user/hive/warehouse/profiling.db/"
  val dataPath = s"$hdfsUri/user/bigdata/profiling/"

  // [2016-06-02 06:04:28:326 INFO delegation daily incremental lines: 77514
  // [2016-06-02 06:06:59:850 INFO touring daily incremental lines: 11592
  // [2016-06-02 06:09:16:684 INFO touring_house daily incremental lines: 26323
  // [2016-06-02 06:12:27:378 INFO contract daily incremental lines: 3697
  // [2016-06-02 06:15:00:996 INFO house daily incremental lines: 92779 // todo: ? 92779

  val dailyMaxIncLines = Map("delegation" -> 20 * 10000,
                             "touring" -> 20 * 10000,
                             "touring_house" -> 20 * 10000,
                             "contract" -> 10 * 10000,
                             "house" -> 50 * 10000)

  def main(args: Array[String]) {
    new Batch().parseArgs(args).asInstanceOf[Batch].start()
  }

  def run(sc: SparkContext,
          date: String, isColdStart: Boolean,
          runSpecified: Boolean,
          runDel: Boolean,
          runTouring: Boolean,
          runTouringHouse: Boolean,
          runContract: Boolean,
          runHouse: Boolean,
          isParallel: Boolean) {

    val oneDayBefore = DateUtil.dateDiff(date, -1)
    val dateToClear = DateUtil.dateDiff(date, -60)

    val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(Batch.hdfsUri), hadoopConf)

    val logger = LoggerFactory.getLogger(Batch.getClass.getName, Map.empty[String, String].asJava)

    import HqlFactory._
    import RowParser._

    import concurrent.ExecutionContext.Implicits.global

    def registerCompletion(f: Future[_], taskName: String) = {
      f.onComplete {
                     case Success(results) => logger.info(s"$taskName task succeeded.")
                     case Failure(ex) => logger.info(s"$taskName task failed. ex: ${ex.getMessage}, stacktrace: ${ExUtil.getStackTrace(ex)}")
                   }
      f
    }

    def deletePath(path: String) {
      try {
        logger.info(s"deleting $path")
        if (path.length < Batch.hiveDbPath.length) {
          logger.info(s"$path is to short!")
          return
        }
        // recursive delete
        hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
      } catch {
        case _: Throwable =>
          logger.info(s"deleting $path failed.")
      }
    }

    def runTask(taskName: String,
                target: String,
                hqlFacMethod: String => String,
                rowParser: String => Seq[UpdateRequest]) = {
      Future {
               hiveCtx.sql(HqlFactory.getDeletion("profiling", s"$taskName$dateToClear"))

               val data = hiveCtx.sql(hqlFacMethod(date))
               if (isColdStart) {
                 logger.info(s"$taskName, loading total data...")
                 Batch.parseAndIndex(sc.getConf.getAll.toMap, data.map(_.mkString("\t")), target, rowParser)
               } else {
                 val inc = Batch.getInc(hiveCtx.sparkContext,
                                        data.map(_.mkString("\t")),
                                        hiveCtx.read.table(s"profiling.$taskName$oneDayBefore").map(_.mkString("\t")),
                                        date,
                                        oneDayBefore,
                                        taskName)

                 inc.saveAsTextFile(s"$dataPath/$taskName$date")

                 val count = inc.count()
                 logger.info(s"$taskName daily incremental lines: $count")

                 if(count < dailyMaxIncLines.get(taskName).get) {
                   Batch.parseAndIndex(sc.getConf.getAll.toMap, inc, target, rowParser)
                   Batch.parseAndUpdateRedis(sc.getConf.getAll.toMap, inc, taskName, date)
                 }
                 else logger.warn(s"$taskName daily inc lines: $count, aborting...")

                 SMSUtil.sendMail(s"$taskName $date inc", count)
               }

               data
             } map { data =>

        hiveCtx.sql(HqlFactory.getDeletion("profiling", s"$taskName$date"))
        deletePath(s"${Batch.hiveDbPath}$taskName$date")

        // save每日全量数据至hive表, 用于明天例行任务join以获得增量
        logger.info(s"declare saving data to table: profiling.$taskName$date")
        data.write.saveAsTable(s"profiling.$taskName$date")
      }
    }

    def getDelTask          = runTask("delegation",     "customer/customer/delegations",      getDelegation,    parseDel)
    def getTouringTask      = runTask("touring",        "customer/customer/tourings",         getTouring,       parseTouring)
    def getTouringHouseTask = runTask("touring_house",  "customer/customer/tourings.houses",  getTouringHouse,  parseTouringHouse)
    def getContractTask     = runTask("contract",       "customer/customer/contracts",        getContract,      parseContract)
    def getHouseTask        = runTask("house",          "house/house",                        getHouse,         parseHouse)

    if (runSpecified) {
      if (runDel) {
        val f = registerCompletion(getDelTask, "delegation")
        Await.result(f, Batch.batchTimeOutHr.hours)
      }

      if (runTouring) {

        val f = registerCompletion(getTouringTask, "touring")
        Await.result(f, Batch.batchTimeOutHr.hours)
      }

      if (runTouringHouse) {
        val f = registerCompletion(getTouringHouseTask, "touring_house")
        Await.result(f, Batch.batchTimeOutHr.hours)
      }

      if (runContract) {
        val f = registerCompletion(getContractTask, "contract")
        Await.result(f, Batch.batchTimeOutHr.hours)
      }

      if (runHouse) {
        val f = registerCompletion(getHouseTask, "house")
        Await.result(f, Batch.batchTimeOutHr.hours)
      }
    } else if (isParallel) {

      val delegationTask = getDelTask
      val touringTask = getTouringTask
      val touringHouseTask = getTouringHouseTask
      val contractTask = getContractTask
      val HouseTask = getHouseTask

      registerCompletion(delegationTask, "delegation")
      registerCompletion(touringTask, "touring")
      registerCompletion(touringHouseTask, "touring_house")
      registerCompletion(contractTask, "contract")
      registerCompletion(HouseTask, "house")

      Await.result(Future.sequence(List(delegationTask, touringTask, touringHouseTask, contractTask, HouseTask)), Batch.batchTimeOutHr.hour)

    } else {
      val f = for (_ <- registerCompletion(getDelTask, "delegation");
                   _ <- registerCompletion(getTouringTask, "touring");
                   _ <- registerCompletion(getTouringHouseTask, "touring_house");
                   _ <- registerCompletion(getContractTask, "contract");
                   end <- registerCompletion(getHouseTask, "house")) yield end

      Await.result(f, Batch.batchTimeOutHr.hours)
    }
  }

  def getInc(sc: SparkContext, curPath: String, refPath: String, date: String, refDate: String, taskName: String): RDD[String] =
    getInc(sc, sc.textFile(curPath), sc.textFile(refPath), date, refDate, taskName)

  def getInc(sc: SparkContext, curRDD: RDD[String], refRDD: RDD[String], date: String, refDate: String, taskName: String): RDD[String] = {
    curRDD.map((_, date))
    .union(refRDD.map((_, refDate)))
    .map(pair => {
      /**
        * pair格式: (line, date).
        * return格式: (hash, (line, date)).
        * 另外, building_area, price等字段有时会变化, hql中加上了round
        *
        * 忽略touring_id和下面的touring_house_id的变化, 因为经常变. 一天600多万.
        * 影响要看出现过的id之后会不会出现, 另外就是touring相关的id之后用新id可能查不到.
        *
        */
      if (taskName == "touring") {
        val parts = pair._1.split("\t")
        // phone + showing_begin_time + showing_end_time
        ((parts(0) + parts(5) + parts(6)).toLowerCase().hashCode, pair)

      } else if (taskName == "touring_house") {
        val parts = pair._1.split("\t")
        // phone + created_time + house_id
        ((parts(0) + parts(5) + parts(8)).toLowerCase().hashCode, pair)

      } else {
        (pair._1.toLowerCase().hashCode, pair)
      }
    }) groupByKey() filter { pair => // 相同的行group在一起
      val (_, iter) = pair
      val seq = iter.seq
      seq.size match {
        case 1 => seq.head._2 match {
          // 唯一的行
          case `date` => true
          case _ => false
        }
        case _ => false // 重复的行. 忽略hashCode碰撞
      }
    } map { pair =>
      val (_, seq) = pair
      seq.head._1
    }
  }

  def parseAndIndex(conf: Map[String, String], rdd: RDD[String], index: String, parse: String => Seq[UpdateRequest]) {
    rdd.foreachPartition { lines =>
      val logger = getLogger(classOf[Batch].getName, conf)
      var seq = 0L
      try {
        val proxy = new BlockingBackoffRetryProxy(conf)
        for (line <- lines) {
          try {
            val reqs = parse(line)
            for(req <- reqs)
              proxy.send(req)
          } catch {
            case ex: Exception =>
              logger.info(s"parse index $index failed, rec seq: $seq, line: $line", ex)
          }
          seq += 1
        }
        proxy.flush()
      } catch {
        case ex: Exception =>
          logger.info(s"parse index $index failed, rec seq: $seq", ex)
      }
    }
  }

  def parseAndUpdateRedis(conf: Map[String, String], rdd: RDD[String], taskName: String, date: String) {
    val logger = getLogger(classOf[Batch].getName, conf)
    if(taskName == "touring_house") {
      val redisClient = new PipelinedJedisClient
      logger.info("starting redis update, touring_house...")
      rdd map {line => (line.split("\t")(2), 1) } reduceByKey { (x, y) => x + y } foreachPartition { case tuples =>
        for (tuple <- tuples) {
          val (houseId, cnt) = tuple
          redisClient.send(s"tr_${houseId}_${DateUtil.getOneDayBefore(DateUtil.parseDate(date))}", s"tr_${houseId}_$date", cnt)
        }
      }
      redisClient.kvFlush()
    } else if(taskName == "house") {
      val redisClient = new PipelinedJedisClient
      logger.info("starting redis update, house...")
      rdd foreachPartition { lines =>
        for (line <- lines) {
          val parts = line.split("\t")
          redisClient.kvSend(s"info_${parts(0)}", s"${parts(39)}\t${parts(19)}")
        }
      }
      redisClient.kvFlush()
    }
  }
}

class Batch extends BatchBase {
  def start() {
    val logger = LoggerFactory.getLogger(Batch.getClass.getName, Map.empty[String, String].asJava)

    val ha = if (!isColdStart) new BatchHAManager(date) else null
    if(!isColdStart) ha.pollingUntilBecomeMasterUnlessJobFinished()

    val sc = getSc

    val dependencies = new Dependencies(date)
    dependencies.waitUtilReady(sc.getConf.get("spark.hive.table.waitMs", "60000").toLong)

    val startMs = System.currentTimeMillis()
    sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
        logger.info("application end time: %s, start time: %s",
                    applicationEnd.time.toString, startMs.toString) // info接口是java type
        if (applicationEnd.time - startMs > 180000 && !isColdStart) // app时间小于3min认为不正常.
          if(!isColdStart) ha.markJobAsFinished()
      }
    })

    val customerIdx = Properties.getOrDefault("spark.es.idx.customer", "customer")
    val delIdx = Properties.getOrDefault("spark.es.idx.delegation", "customer_delegation")
    val touringIdx = Properties.getOrDefault("spark.es.idx.touring", "customer_touring")
    val contractIdx = Properties.getOrDefault("spark.es.idx.contract", "customer_contract")
    val houseIdx = Properties.getOrDefault("spark.es.idx.house", "house")

    val Indices = Array(customerIdx, delIdx, touringIdx, contractIdx, houseIdx)
    logger.info(s"indices: ${Indices.mkString(",")}")

    import Batch._

    val idxMgr = new IndexManager(Map.empty[String, String].asJava)
    Indices foreach { index =>
      idxMgr.setReplicas(index, 0)
      idxMgr.setRefreshInterval(index, -1)
    }

    try {
      run(sc, date, isColdStart, runSpecified, runDel, runTouring, runTouringHouse, runContract, runHouse, isParallel)
    } finally {
      Indices foreach { index =>
        idxMgr.forceMerge(index)
        idxMgr.setReplicas(index, Batch.indexReplicas)
        idxMgr.setRefreshInterval(index, Batch.IndexRefreshInterval)
      }

      sc.stop()
    }
  }
}

