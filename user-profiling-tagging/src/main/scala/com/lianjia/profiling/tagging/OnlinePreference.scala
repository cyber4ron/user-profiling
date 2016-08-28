package com.lianjia.profiling.tagging

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.tagging.features.Features.EventType
import com.lianjia.profiling.tagging.features.UserPreference
import com.lianjia.profiling.tagging.tag.UserTag
import com.lianjia.profiling.tagging.user.OnlineEventTagging
import com.lianjia.profiling.util.{ZipUtil, DateUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object OnlinePreference {
  val logger = LoggerFactory.getLogger(OnlinePreference.getClass)

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

  def combo = (prefer: UserPreference, prefer2: UserPreference) => {
    prefer.merge(prefer2)
    prefer.updateMeta(UserTag.WRITE_TS, System.currentTimeMillis)
    prefer
  }

  def seq = (prefer: UserPreference, evts: String) => {
    val events = evts.split("\t")
    for (event <- events) {
      try {
        val obj = JSON.parseObject(event)
        if (obj.containsKey("ucid")) prefer.updateMeta(UserTag.UCID, obj.get("ucid"))
        if (obj.containsKey("uuid")) prefer.updateMeta(UserTag.UCID, obj.get("uuid"))

        prefer.updateMeta(UserTag.WRITE_TS, System.currentTimeMillis.toString)
        obj.get("evt") match {
          case "dtl" => OnlineEventTagging.onDetail(prefer, obj, EventType.PC_DETAIL)
          case "fl" => OnlineEventTagging.onFollow(prefer, obj, EventType.PC_FOLLOW)
          case "mob_dtl" => OnlineEventTagging.onDetail(prefer, obj, EventType.PC_DETAIL)
          case "mob_fl" => OnlineEventTagging.onFollow(prefer, obj, EventType.PC_FOLLOW)
          case _ =>
        }
      } catch {
        case ex: Throwable =>
          System.err.println("event: " + event)
          System.err.println("ex: " + ex)
      }
    }

    prefer
  }

  def updatePrefer(base: RDD[UserPreference], inc: RDD[String], uuidDict: RDD[String]) = {
    val events = inc flatMap { line: String =>
      try {
        val Array(id, evts) = line.split("\t", 2)
        Array[(String, String)]((id, ZipUtil.inflate(evts)))
      } catch {
        case ex: Throwable =>
          System.err.println("line: " + line)
          System.err.println("ex: " + ex)
          Array.empty[(String, String)]
      }
    }

    events.aggregateByKey(new UserPreference())(seq, combo)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("profiling-online-user-prefer")
    val sc = new SparkContext(conf)

    val date = DateUtil.getYesterday
    val oneDayBefore = DateUtil.getOneDayBefore(DateUtil.parseDate(date))

    val base = sc.objectFile(s"/user/bigdata/profiling/online_user_prefer_$oneDayBefore")
               .asInstanceOf[RDD[UserPreference]]

    val inc = sc.textFile(s"/user/bigdata/profiling/online_user_assoc_$date")

    val uuidDict = sc.textFile(s"/user/bigdata/profiling/online_user_uuid_dict_$date")

    val prefer = updatePrefer(base, inc, uuidDict)

    val preferObjPath = s"/user/bigdata/profiling/online_user_prefer_$date"
    val preferTextPath = s"/user/bigdata/profiling/online_user_prefer_${date}_txt"

    removePath(preferObjPath)
    removePath(preferTextPath)

    prefer saveAsObjectFile preferObjPath
    prefer saveAsTextFile preferTextPath

    sc.stop()
  }
}
