package com.lianjia.profiling.tagging

import java.sql.DriverManager
import java.util.concurrent.ThreadLocalRandom

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.util.{DateUtil, ZipUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.update.UpdateRequest
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
  * @author fenglei@lianjia.com on 2016-06
  */

/**
  * 似乎网络不稳定, stage完成速度时快时慢, 另会出现间歇性executor lost之类错误. 增加重试次数.
  * 另会出现shuffle.MetadataFetchFailedException错误, 导致stage失败. 可能是driver内存不足,或是网络问题?
  */
object AssocUcidAndUuid extends App {
  val logger = LoggerFactory.getLogger(AssocUcidAndUuid.getClass)

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

  val conf = new SparkConf().setAppName("profiling-online-user-assoc")
  if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")
  val sc = new SparkContext(conf)

  val date = sc.getConf.get("spark.backtrace.onlineUser.endDate")
  val oneDayBeforeDate = DateUtil.getOneDayBefore(DateUtil.parseDate(date))

  val days = sc.getConf.get("spark.backtrace.days").toInt
  val dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(i)))
                  .map(dt => s"$hdfsUri/user/bigdata/profiling/online_events_$dt")

  val housePath = s"$hdfsUri/user/bigdata/profiling/house_$date"

  val parallel = Math.max(32, 512 * (days.toDouble / 31)).toInt

  logger.info(s"date: $date")
  logger.info(s"oneDayBeforeDate: $oneDayBeforeDate")
  logger.info(s"data paths: $dataPaths")
  logger.info(s"parallel: $parallel")
  logger.info(s"housePath: $housePath")

  def mergeUuidDict(base: RDD[String], inc: RDD[(String, (String, String, Long))]): RDD[(String, (String, String, Long))] = {
    val tsToRemove = new DateTime(System.currentTimeMillis).minusDays(360).getMillis

    base map { line: String =>
      val Array(uuid, ucid, source, ts) = line.split("\t")
      (uuid, (ucid, source, ts.toLong))
    } union inc reduceByKey { case ((ucid1, source1, ts1), (ucid2, source2, ts2)) =>
      if (ts1 > ts2) (ucid1, source1, ts1)
      else (ucid2, source2, ts2)
    } filter { case (_, (_, _, ts)) =>
      ts > tsToRemove
    }
  }

  /**
    * 算一个月的话6w+个task
    */
  def getIncUuidDict(data: RDD[String], partitions: Int): RDD[(String, (String, String ,Long))] = {
    data flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val source = if (parts(0).startsWith("mob")) "mob" else "pc"
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if (obj.containsKey("ucid") && obj.containsKey("uuid")) {
          val ts = if (obj.containsKey("ts")) obj.get("ts").toString.toLong else System.currentTimeMillis
          Array[(String, (String, String, Long))]((obj.get("uuid").toString, (obj.get("ucid").toString, source, ts)))
        } else Array[(String, (String, String, Long))]()
      }
      else Array[(String, (String, String, Long))]()
    } coalesce (partitions, false) reduceByKey { case ((ucid1, source1, ts1), (ucid2, source2, ts2)) =>
      if(ts1 > ts2) (ucid1, source1, ts1)
      else (ucid2, source2, ts2)
    }
  }

  def assocByDict(sc: SparkContext, houses: RDD[(String, String)],
                  uuidDict: RDD[(String, String)],
                  data: RDD[String]): RDD[(String, String)] = {
    // extract user id, -> (user_id, evt)
    val events = data flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        obj.put("evt", parts(0))
        if (obj.containsKey("ucid")) {
          val id = obj.get("ucid").toString
          Array[(String, String)]((id, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
        }
        else if (obj.containsKey("uuid")) {
          Array[(String, String)]((obj.get("uuid").toString, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
        }
        else Array.empty[(String, String)]
      }
      else Array[(String, String)]()
    } coalesce (parallel, false)

    // associate ucid, -> (house_id, evt)
    val eventsAssocUcid = events leftOuterJoin (uuidDict, parallel) flatMap {
      case (id, (evt, ucid)) =>
        val obj = JSON.parse(evt).asInstanceOf[JSONObject]
        if(ucid.isDefined) {
          obj.put("ucid", ucid.get)
          obj.put("assoc", true)
        }

        if(obj.containsKey("dtl_id") && obj.containsKey("dtl_type") && obj.get("dtl_type").toString == "1") {
          Array[(String, String)]((obj.get("dtl_id").toString,
                                    JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))

        } else if(obj.containsKey("fl_id") && obj.containsKey("fl_type") && obj.get("fl_type").toString == "2") {
          Array[(String, String)]((obj.get("fl_id").toString,
                                    JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))

        } else Array[(String, String)]((ThreadLocalRandom.current().nextInt(Int.MinValue + 1, 0).toString,
                                         JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
    }

    // join house, -> (user_id, evts)
    val eventsWithHouse = eventsAssocUcid leftOuterJoin houses map {
      case (_, (evt, house)) =>
        val obj = JSON.parse(evt).asInstanceOf[JSONObject]

        if (house.isDefined)
          obj.put("house", JSON.parse(house.get))
        else if (obj.containsKey("dtl_id") && obj.containsKey("dtl_type") && obj.get("dtl_type").toString == "1"
          || obj.containsKey("fl_id") && obj.containsKey("fl_type") && obj.get("fl_type").toString == "2")
          obj.put("house", new JSONObject())

        val id = if (obj.containsKey("ucid")) obj.get("ucid").toString else obj.get("uuid").toString

        (id, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames))
    }

    // group by user id
    val maxEventLen = 10 * 1000 * 1000
    eventsWithHouse reduceByKey { case (x, y) =>
      if (x.length > maxEventLen) x
      else if (y.length > maxEventLen) y
      else s"$x\t$y"
    } map { case (id, evts) => (id, ZipUtil.deflate(evts.getBytes)) }
  }

  val uuidDictOriginBasePath = s"$hdfsUri/user/bigdata/profiling/online_user_uuid_dict_$oneDayBeforeDate"
  val uuidDictBasePath = s"$hdfsUri/user/bigdata/profiling/online_user_uuid_dict_$date"
  val associatedPath = s"$hdfsUri/user/bigdata/profiling/online_user_assoc_$date"

  logger.info("path: " + uuidDictOriginBasePath)
  logger.info("path: " + uuidDictBasePath)
  logger.info("path: " + associatedPath)

  val uuidDictOriginBase = sc.textFile(uuidDictOriginBasePath)
  val dailyData = sc.textFile(dataPaths.mkString(","))

  /**
    *
    */
  def countJoinedUuids(data : RDD[String], uuidDict: RDD[String]) = {
    val uuids = data map { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if (obj.containsKey("uuid") && !obj.containsKey("ucid")) {
          (obj.get("uuid").toString, "")
        }
        else ("", "")
      }
      else ("", "")
    } filter (!_._1.isEmpty) coalesce (parallel, false) // 647013820

    data map { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if (obj.containsKey("ucid")) {
          (obj.get("ucid").toString, "")
        }
        else ("", "")
      }
      else ("", "")
    } filter (!_._1.isEmpty) coalesce (parallel, false) count()

    // data all: count 781407773

    uuids join (uuidDict map { line: String =>
      val Array(uuid, _, _, _) = line.split("\t")
      (uuid, "")
    } groupByKey() map { case (uuid, _) => (uuid, "") }) count()

    // 7yue, 37196536 / 647013820, 6%

    // Array[Int] = Array(1, 2, 2)
    // sc.makeRDD(Array((1, 0), (2, 0), (2, 0), (3, 0))) join sc.makeRDD(Array((1, 0), (2, 0), (4, 0))) map {case (id, _) => id} collect()

    // Array[(Int, Int)] = Array((1,0), (2,0), (3,0))
    // sc.makeRDD(Array((1, 0), (2, 0), (2, 0), (3, 0))) groupByKey() map { case (uuid, _) => (uuid, 0) } collect()
  }

  // update uuid dict

  removePath(uuidDictBasePath)

  val uuidDictRDD = mergeUuidDict(uuidDictOriginBase, getIncUuidDict(dailyData, parallel))

  import scala.collection.JavaConverters._
  val config = sc.getConf.getAll.toMap
  uuidDictRDD foreachPartition { part =>
    try {
      val esProxy = new BlockingBackoffRetryProxy(config)
      for ((uuid, (ucid, source, ts)) <- part) {
        esProxy.send(new UpdateRequest("uuid_ucid_map", "mp", uuid)
                     .docAsUpsert(true)
                     .doc(Map("uuid" -> uuid,
                              "ucid" -> ucid,
                              "src" -> source,
                              "ts" -> ts).asJava))
      }
      esProxy.flush()
    } catch {
      case ex: Throwable => println(ex.getStackTraceString)
    }
  }

  val uuidDictToSave = uuidDictRDD map { case (uuid, (ucid, source, ts)) =>
    s"$uuid\t$ucid\t$source\t$ts"
  } coalesce (32, false)

  uuidDictToSave.persist(StorageLevel.MEMORY_AND_DISK_SER)
  uuidDictToSave saveAsTextFile uuidDictBasePath

  // add hive table partition
  val conn = DriverManager.getConnection(s"jdbc:hive2://jx-bd-hadoop03.lianjia.com:10000/profiling", "", "")
  val stmt = conn.createStatement

  var sql = s"alter table profiling.uuid_dict drop if exists partition(pt='${date}000000')"
  logger.info(s"executing: $sql")
  stmt.execute(sql)

  val pathForHive = s"/user/bigdata/profiling/online_user_uuid_dict_${date}_hive_ext"
  uuidDictToSave saveAsTextFile pathForHive

  sql = s"alter table profiling.uuid_dict add partition(pt='${date}000000') location '$pathForHive'"
  logger.info(s"executing: $sql")
  stmt.execute(sql)

  sc.textFile("/user/bigdata/profiling/online_user_assoc_feat_part_inflate") map { line =>
    val parts = line.split("\t", 2)
    val id = parts(0)
    val data = parts(1)
    val json = data.split("\t")(0)
    val obj = JSON.parseObject(json)
    val ucid = if(obj.containsKey("ucid")) obj.getString("ucid") else obj.getString("ucid_mark")
    (ucid, (id, data))
  } groupByKey() flatMap { case (ucid, entities) =>
    if(entities.toSeq.size > 1) entities.map { case(id, data) => s"${ucid}\t${id}\t${data}" }.toArray[String]
    else Array.empty[String]
  } saveAsTextFile "/user/bigdata/profiling/online_user_assoc_feat_part_inflate_group"

  // associate uuid & ucid
  val houses = sc.textFile(housePath) map { line =>
    val Array(id, json) = line.split("\t")
    (id, json)
  }

  removePath(associatedPath)


  assocByDict(sc,
              houses,
              uuidDictRDD map {
                case (uuid, (ucid, source, ts)) => (uuid, ucid)
              },
              dailyData) map {
    case (id, evts) =>
      s"$id\t$evts"
  } saveAsTextFile associatedPath

  sc.stop()
}
