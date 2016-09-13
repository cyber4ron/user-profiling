package com.lianjia.profiling.tool

import java.util.concurrent.ThreadLocalRandom

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lianjia.profiling.tagging.batch.AssocUserOnlineEvents
import com.lianjia.profiling.util.ZipUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object AssocUcidAndUuidTest extends App {

  def test(sc: SparkContext) {
    val associated = assoc(sc, sc.parallelize(Array(
      "001\t" + """{"ucid": "x01", "uuid": "x02", "ch_id": "c01"}""",
      "002\t" + """{"uuid": "x02", "ch_id": "c02"}""",
      "003\t" + """{"uuid": "x02", "ch_id": "c03"}""",
      "004\t" + """{"uuid": "x03", "ch_id": "c04"}""")))
    val res = associated.collect()
    println(res map { case ((x, y)) => s"$x\t$y" } mkString "\t")
  }

  def test2(sc: SparkContext) {
    val houses = sc.parallelize(Array(
      ("house001", """{"house_id": "house001", "uuid": "x02", "ch_id": "c01"}"""),
      ("house002", """{"house_id": "house002", "ch_id": "c02"}"""),
      ("house003", """{"house_id": "house003", "ch_id": "c03"}"""),
      ("house004", """{"house_id": "house004", "ch_id": "c04"}""")))

    val data = sc.parallelize(Array(
      "001\t" + """{"ucid": "x01", "uuid": "x02", "dtl_type": "1", "dtl_id": "house002", "ch_id": "c01"}""",
      "002\t" + """{"uuid": "x02", "dtl_type": "1", "dtl_id": "house001", "ch_id": "c02"}""",
      "003\t" +
        """{"uuid": "x02", "ch_id":
          | "c03"}""".stripMargin,
      "004\t" + """{"uuid": "x03", "dtl_type": "1", "dtl_id": "house003", "ch_id": "c04"}"""))

    val assoc = data flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if(obj.containsKey("dtl_id") && obj.containsKey("dtl_type") && obj.get("dtl_type") == "1") {
          Array[(String, String)]((obj.get("dtl_id").toString, parts(1)))
        } else if(obj.containsKey("fl_id") && obj.containsKey("fl_type") && obj.get("fl_type") == "1") {
          Array[(String, String)]((obj.get("fl_id").toString, parts(1)))
        } else Array[(String, String)]((ThreadLocalRandom.current().nextInt(-10000000, 0).toString, parts(1)))
      }
      else Array[(String, String)]()
    } leftOuterJoin houses flatMap { case (_, (event, house)) =>
      val obj = JSON.parse(event).asInstanceOf[JSONObject]
      if (obj.containsKey("uuid")) {
        if (house.isDefined) obj.put("house", JSON.parse(house.get))
        else obj.put("house", "")
        Array[(String, String)]((obj.get("uuid").toString, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
      }
      else Array.empty[(String, String)]
    }

    val res = assoc.collect()

    println()
  }

  def test3(sc: SparkContext) {
    val houses = sc.parallelize(Array(
      ("house001", """{"house_id": "house001", "uuid": "x02", "ch_id": "c01"}"""),
      ("house002", """{"house_id": "house002", "ch_id": "c02"}"""),
      ("house003", """{"house_id": "house003", "ch_id": "c03"}"""),
      ("house004", """{"house_id": "house004", "ch_id": "c04"}""")))

    val data = sc.parallelize(Array(
      "001\t" + """{"ucid": "x01", "uuid": "x02", "dtl_type": "1", "dtl_id": "house002", "ch_id": "c01"}""",
      "002\t" + """{"uuid": "x02", "dtl_type": "1", "dtl_id": "house001", "ch_id": "c02"}""",
      "003\t" + """{"uuid": "x02", "ch_id":"c03"}""",
      "004\t" + """{"uuid": "x03", "dtl_type": "1", "dtl_id": "house003", "ch_id": "c04"}"""))

    val uuidDict = Map(("x02", "x01"))
    val uuidDictBroadcast = sc.broadcast(uuidDict).value

    val assoc = data flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if(obj.containsKey("dtl_id") && obj.containsKey("dtl_type") && obj.get("dtl_type").toString == "1") {
          Array[(String, String)]((obj.get("dtl_id").toString, parts(1)))
        } else if(obj.containsKey("fl_id") && obj.containsKey("fl_type") && obj.get("fl_type").toString == "2") {
          Array[(String, String)]((obj.get("fl_id").toString, parts(1)))
        } else Array[(String, String)]((ThreadLocalRandom.current().nextInt(-10000000, 0).toString, parts(1)))
      }
      else Array[(String, String)]()
    } leftOuterJoin houses flatMap { case (_, (event, house)) =>
      val obj = JSON.parse(event).asInstanceOf[JSONObject]
      if (obj.containsKey("ucid")) {
        val id = obj.get("ucid").toString
        if (house.isDefined) obj.put("house", JSON.parse(house.get))
        else obj.put("house", new JSONObject())
        Array[(String, String)]((id, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
      }
      else if (obj.containsKey("uuid")) {
        if (house.isDefined) obj.put("house", JSON.parse(house.get))
        else obj.put("house", new JSONObject())

        val ucid = uuidDictBroadcast.get(obj.get("uuid").toString)
        if (ucid.isDefined) obj.put("ucid", ucid.get)
        val id = ucid.getOrElse(obj.get("uuid").toString)
        Array[(String, String)]((id, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
      }
      else Array.empty[(String, String)]
    } coalesce (32, false) reduceByKey { case (x, y) =>
      if (x.length > 100000000) x
      else if (y.length > 100000000) y
      else s"$x\t$y"
    }

    assoc.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val res = assoc.collect()

    println()
  }


  def assoc(sc: SparkContext, data: RDD[String]): RDD[(String, String)] = {
    val houses = sc.textFile(AssocUserOnlineEvents.housePath) map { line =>
      val Array(id, json) = line.split("\t")
      (id, json)
    }

    data flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if(obj.containsKey("dtl_id") && obj.containsKey("dtl_type") && obj.get("dtl_type").toString == "1") {
          Array[(String, String)]((obj.get("dtl_id").toString, parts(1)))
        } else if(obj.containsKey("fl_id") && obj.containsKey("fl_type") && obj.get("fl_type").toString == "2") {
          Array[(String, String)]((obj.get("fl_id").toString, parts(1)))
        } else Array[(String, String)]((ThreadLocalRandom.current().nextInt(-10000000, 0).toString, parts(1)))
      }
      else Array[(String, String)]()
    } leftOuterJoin houses flatMap { case (_, (event, house)) =>
      val obj = JSON.parse(event).asInstanceOf[JSONObject]
      if (obj.containsKey("uuid")) {
        obj.put("house", house.getOrElse(""))
        Array[(String, String)]((obj.get("uuid").toString, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
      }
      else Array.empty[(String, String)]
    } reduceByKey { case (x, y) =>
      if (x.length > 100000000) x
      else if (y.length > 100000000) y
      else s"$x\t$y"
    } flatMap { case ((uuid, evts)) =>
      evts.split("\t") map { evt =>
        JSON.parse(evt).asInstanceOf[JSONObject]
      } collectFirst { case obj if obj.containsKey("ucid") =>
        obj.get("ucid").toString
      } map { ucid =>
        Array[(String, String)]((ucid, evts))
      } getOrElse {
                    Array[(String, String)]((uuid, evts))
                  }.asInstanceOf[Array[(String, String)]] // 去掉asInstanceOf的话IDE不识别
    } reduceByKey { case (x, y) =>
      if (x.length > 100000000) x
      else if (y.length > 100000000) y
      else s"$x\t$y"
    }
  }
  def computeUuidDictTmp(sc: SparkContext) {
    var endDate: DateTime = null
    var days: Int = 0
    var dataPaths: Seq[String] = null

/*    endDate = DateUtil.parseDate("20160531")
    days = 31
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201605"

    endDate = DateUtil.parseDate("20160430")
    days = 30
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201604"

    endDate = DateUtil.parseDate("20160331")
    days = 31
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201603"

    endDate = DateUtil.parseDate("20160228")
    days = 28
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201602"

    endDate = DateUtil.parseDate("20160228")
    days = 28
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201602"

    endDate = DateUtil.parseDate("20160131")
    days = 31
    dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(endDate.minusDays(i)))
                .map(date => s"/user/bigdata/profiling/online_events_$date")
    getIncUuidDict(sc.textFile(dataPaths.mkString(",")), 128) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_201601"

    // merge
    mergeUuidDict(sc.textFile(s"/user/bigdata/profiling/online_user_uuid_dict_05"),
                  sc.textFile(s"/user/bigdata/profiling/online_user_uuid_dict_201607") map { line =>
                    val Array(uuid, ucid, ts) = line.split("\t")
                    (uuid, (ucid, ts.toLong))
                  }) map {
      x => s"${x._1}\t${x._2._1}\t${x._2._2}"
    } saveAsTextFile s"/user/bigdata/profiling/online_user_uuid_dict_06"*/
  }

  val sc: SparkContext = null
  sc.textFile("/user/bigdata/profiling/online_user_assoc_2016063030/part-00412") map { line =>
    val Array(id, evts) = line.split("\t") // todo: 有match失败情况, try-catch
    s"$id\t${ZipUtil.inflate(evts)}"
  } saveAsTextFile("/user/bigdata/profiling/online_user_assoc_2016063030_inflate2")
}
