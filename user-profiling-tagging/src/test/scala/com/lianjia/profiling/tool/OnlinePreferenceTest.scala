package com.lianjia.profiling.tool

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lianjia.profiling.common.hbase.DumpHFile
import com.lianjia.profiling.tagging.features.UserPreference
import com.lianjia.profiling.tagging.tag.UserTag
import com.lianjia.profiling.util.DateUtil
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object OnlinePreferenceTest {

  val conf = new SparkConf().setAppName("online-user-prefer-batch")
  val sc = new SparkContext(conf)

  val tableName = "profiling:prefer_online"

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val housePath = s"$hdfsUri/user/bigdata/profiling/house_20160621"
  val onlineDetailPath = s"$hdfsUri/user/bigdata/profiling/online_user_20160530/dtl_20160605"
  val onlineFollowPath = s"$hdfsUri/user/bigdata/profiling/online_user_20160530/fl_20160605"
  val onlineMobileDetailPath = s"$hdfsUri/user/bigdata/profiling/online_user_20160530/mob_dtl_20160605"
  val onlineMobileFollowPath = s"$hdfsUri/user/bigdata/profiling/online_user_20160530/mob_fl_20160605"

  val onlineBasePath = s"$hdfsUri/user/bigdata/profiling/online_user_base_%day"
  val onlineBaseJsonPath = s"$hdfsUri/user/bigdata/profiling/online_user_base_json_%day"
  val onlineBaseHFilePath = s"$hdfsUri/user/bigdata/profiling/online_user_base_hfile_%day"

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUri), hadoopConf)

  val partitions = 256

  case class Event(ucid: String, uuid: String, eventType: String, houseId: String, ts: Long)
  case class EventAndHouse(event: Event, house: String)

  def getPCFollow(house: RDD[(String, String)], path: String) = {
    sc.textFile(path) flatMap { line =>
      val Array(_, json) = line.split("\t")

      val obj = JSON.parse(json).asInstanceOf[JSONObject]

      if (!obj.containsKey("fl_type") || obj.get("fl_type").toString != "2") Array.empty[(String, Event)]
      else {
        val (ucid, uuid) = getId(obj)
        if ((ucid.isEmpty && uuid.isEmpty) || !obj.containsKey("fl_id") || !obj.containsKey("ts")) Array.empty[(String, Event)]
        else Array((obj.get("fl_id").toString, Event(ucid, uuid, "fl", obj.get("fl_id").toString,
                                                     java.lang.Long.parseLong(obj.get("ts").toString))))
      }
    } groupByKey partitions map {
      case (id, follows) =>
        if(follows.size > 200) (id, follows.slice(0, 200))
        else (id, follows)
    } flatMap {
      case (id, follows) =>
        follows.map((id, _))
    } join(house, partitions) map {
      case (houseId, (event, houseInfo)) =>
        (if (!event.ucid.isEmpty) event.ucid else event.uuid, EventAndHouse(event, houseInfo))
    }
  }

  def getPCDetail(house: RDD[(String, String)], path: String) = {
    sc.textFile(path) flatMap { line =>
      val Array(_, json) = line.split("\t")

      val obj = JSON.parse(json).asInstanceOf[JSONObject]

      if (!obj.containsKey("dtl_type") || obj.get("dtl_type").toString != "1") Array.empty[(String, Event)]
      else {
        val (ucid, uuid) = getId(obj)
        if ((ucid.isEmpty && uuid.isEmpty) || !obj.containsKey("dtl_id") || !obj.containsKey("ts")) Array.empty[(String, Event)]
        else Array((obj.get("dtl_id").toString, Event(ucid, uuid, "dtl", obj.get("dtl_id").toString,
                                                      java.lang.Long.parseLong(obj.get("ts").toString))))
      }
    } groupByKey partitions map {
      case (id, details) =>
        if(details.size > 200) (id, details.slice(0, 200))
        else (id, details)
    } flatMap {
      case (id, details) =>
        details.map((id, _))
    } join(house, partitions) map {
      case (houseId, (event, houseInfo)) =>
        (if (!event.ucid.isEmpty) event.ucid else event.uuid, EventAndHouse(event, houseInfo))
    }
  }

  def getMobileFollow(house: RDD[(String, String)], path: String) = {
    sc.textFile(path) flatMap { line =>
      val Array(_, json) = line.split("\t")

      val obj = JSON.parse(json).asInstanceOf[JSONObject]

      if (!obj.containsKey("fl_type") || obj.get("fl_type").toString != "2") Array.empty[(String, Event)]
      else {
        val (ucid, uuid) = getId(obj)
        if ((ucid.isEmpty && uuid.isEmpty) || !obj.containsKey("fl_id") || !obj.containsKey("ts")) Array.empty[(String, Event)]
        else Array((obj.get("fl_id").toString, Event(ucid, uuid, "mob_fl", obj.get("fl_id").toString,
                                                     java.lang.Long.parseLong(obj.get("ts").toString))))
      }
    } groupByKey partitions map {
      case (id, follows) =>
        if(follows.size > 200) (id, follows.slice(0, 200))
        else (id, follows)
    } flatMap {
      case (id, follows) =>
        follows.map((id, _))
    } join(house, partitions) map {
      case (houseId, (event, houseInfo)) =>
        (if (!event.ucid.isEmpty) event.ucid else event.uuid, EventAndHouse(event, houseInfo))
    }
  }

  def getMobileDetail(house: RDD[(String, String)], path: String) = {
    sc.textFile(path) flatMap { line =>
      val Array(_, json) = line.split("\t")

      val obj = JSON.parse(json).asInstanceOf[JSONObject]

      if (!obj.containsKey("dtl_type") || obj.get("dtl_type").toString != "1") Array.empty[(String, Event)]
      else {
        val (ucid, uuid) = getId(obj)
        if ((ucid.isEmpty && uuid.isEmpty) || !obj.containsKey("dtl_id") || !obj.containsKey("ts")) Array.empty[(String, Event)]
        else Array((obj.get("dtl_id").toString, Event(ucid, uuid, "mob_dtl", obj.get("dtl_id").toString,
                                                      java.lang.Long.parseLong(obj.get("ts").toString))))
      }
    } groupByKey partitions map {
      case (id, details) =>
        if(details.size > 200) (id, details.slice(0, 200))
        else (id, details)
    } flatMap {
      case (id, details) =>
        details.map((id, _))
    } join(house, partitions) map {
      case (houseId, (event, houseInfo)) =>
        (if (!event.ucid.isEmpty) event.ucid else event.uuid, EventAndHouse(event, houseInfo))
    }
  }

  def getId(obj: java.util.Map[String, Object]) = {
    (if (obj.containsKey("ucid")) obj.get("ucid").toString else "",
      if (obj.containsKey("uuid")) obj.get("uuid").toString else "")
  }

  def seq = (prefer: UserPreference, eventAndHouse: EventAndHouse) => {
    val ts = eventAndHouse.event.ts
    if(!eventAndHouse.event.ucid.isEmpty) prefer.updateMeta(UserTag.UCID, eventAndHouse.event.ucid)
    if(!eventAndHouse.event.uuid.isEmpty) prefer.updateMeta(UserTag.UUID, eventAndHouse.event.uuid)
    prefer.updateMeta(UserTag.WRITE_TS, System.currentTimeMillis.toString)
    try {
      eventAndHouse.event.eventType match {
        case "dtl" =>
        case "fl" =>
        case "mob_dtl" =>
        case "mob_fl" =>
        case _ =>
      }
    } catch {
      case ex: Throwable =>
        System.err.println("====> ex: " + ex)
    }
    prefer
  }

  // get house
  val house = sc.textFile(housePath) map { line =>
    val Array(id, json) = line.split("\t")
    (id, json)
  }

  // get pc detail
  val detail = getPCDetail(house, onlineDetailPath)

  // get pc follow
  val follow = getPCFollow(house, onlineFollowPath)

  // get mobile detail
  val detailMob = getPCFollow(house, onlineMobileDetailPath)

  // get mobile follow
  val followMob = getPCFollow(house, onlineMobileFollowPath)

  val prefer = detail.union(follow).union(detailMob).union(followMob).aggregateByKey(new UserPreference())(seq, combo)
  prefer saveAsObjectFile s"$hdfsUri/user/bigdata/profiling/test333991"

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

  def main(args: Array[String]) {
    // get house
    val house = sc.textFile(housePath) map { line =>
      val Array(id, json) = line.split("\t")
      (id, json)
    }

    // get pc detail
    val detail = getPCDetail(house, onlineDetailPath)

    // get pc follow
    val follow = getPCFollow(house, onlineFollowPath)

    // get mobile detail
    val detailMob = getPCFollow(house, onlineMobileDetailPath)

    // get mobile follow
    val followMob = getPCFollow(house, onlineMobileFollowPath)

    val prefer = detail.union(follow).union(detailMob).union(followMob).aggregateByKey(new UserPreference())(seq, combo)

    val yesterday = DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime().minusDays(1))
    removePath(onlineBasePath.replace("%day", yesterday))
    prefer saveAsObjectFile onlineBasePath.replace("%day", yesterday)
    // 20160530一周, 800w id, 1400s. 46G

    removePath(onlineBaseJsonPath.replace("%day", yesterday))
    prefer map {
      case (id, pref) => s"$id\t${pref.toString}"
    } saveAsTextFile onlineBaseJsonPath.replace("%day", yesterday)

    DumpHFile.saveHFile(prefer map {
      case (id, pref) =>
        (new ImmutableBytesWritable(id.getBytes),
          new KeyValue(id.getBytes, "cf".getBytes(), "cq".getBytes(), pref.serialize()))
    }, tableName, onlineBaseHFilePath.replace("%day", yesterday))

    sc.stop()
  }

  def inc() = {
    val onlineDetailPathTmpl = s"$hdfsUri/user/bigdata/profiling/online_user_%week/dtl_%day"
    val onlineFollowPathTmpl = s"$hdfsUri/user/bigdata/profiling/online_user_%week/fl_%day"
    val onlineMobileDetailPathTmpl = s"$hdfsUri/user/bigdata/profiling/online_user_%week/mob_dtl_%day"
    val onlineMobileFollowPathTmpl = s"$hdfsUri/user/bigdata/profiling/online_user_%week/mob_fl_%day"

    val twoDayBefore = DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime().minusDays(2))
    val yesterday = DateTimeFormat.forPattern("yyyyMMdd").print(new DateTime().minusDays(1))
    val week = DateUtil.alignedByWeek(yesterday)

    // get house
    val house = sc.textFile(housePath) map { line =>
      val Array(id, json) = line.split("\t")
      (id, json)
    }

    val base = sc.objectFile(onlineBasePath.replace("%day", twoDayBefore)) flatMap { prefer: UserPreference =>
      val id =
        if(prefer.getEntries.containsKey(UserTag.UCID)) prefer.getEntries.get(UserTag.UCID).toString
        else if(prefer.getEntries.containsKey(UserTag.UUID )) prefer.getEntries.get(UserTag.UUID).toString
        else ""

      if(id.isEmpty) Array[(String, UserPreference)]()
      else Array((id, prefer))
    }

    // get pc detail
    val detail = getPCDetail(house, onlineDetailPathTmpl.replace("%week", week).replace("%day", yesterday))

    // get pc follow
    val follow = getPCFollow(house, onlineFollowPathTmpl.replace("%week", week).replace("%day", yesterday))

    // get mobile detail
    val detailMob = getPCFollow(house, onlineMobileDetailPathTmpl.replace("%week", week).replace("%day", yesterday))

    // get mobile follow
    val followMob = getPCFollow(house, onlineMobileFollowPathTmpl.replace("%week", week).replace("%day", yesterday))

    val inc = sc.union[(String, EventAndHouse)](detail, follow, detailMob, followMob).aggregateByKey(new UserPreference())(seq, combo)

    val newBase = base leftOuterJoin inc flatMap {
      case (id, (prefer1, prefer2)) =>
        if(prefer2.isDefined) {
          prefer1.updateMeta(UserTag.WRITE_TS, System.currentTimeMillis)
          prefer1.merge(prefer2.get)
        }

        if(prefer2.isEmpty && prefer1.getEntries.containsKey(UserTag.WRITE_TS) &&
          java.lang.Long.parseLong(prefer1.getEntries.get(UserTag.WRITE_TS).toString)
            < System.currentTimeMillis() - 12 * 7 * 24 * 60 * 60 * 1000) Array[(String, UserPreference)]()
        else Array((id, prefer1))
    }

    removePath(onlineBasePath.replace("%day", yesterday))
    newBase saveAsObjectFile onlineBasePath.replace("%day", yesterday)

    removePath(onlineBaseJsonPath.replace("%day", yesterday))
    newBase map {
      case (id, pref) => s"$id\t${pref.toString}"
    } saveAsTextFile onlineBaseJsonPath.replace("%day", yesterday)
  }

  def combo = (prefer: UserPreference, prefer2: UserPreference) => {
    prefer.merge(prefer2)
    prefer.updateMeta(UserTag.WRITE_TS, System.currentTimeMillis)
    prefer
  }

}
