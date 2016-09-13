package com.lianjia.profiling.tagging.batch

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.common.redis.PipelinedJedisClient
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.SparkContext

/**
  * @author fenglei@lianjia.com on 2016-09
  */

object AccumulateInfo {

  val followKeyPrefix = "fl"
  val touringKeyPrefix = "tr"

  /**
    * 从es的db_follow每个房源当日的关注量
    * todo: redis pipelining
    */
  def accDailyHouseFollow(sc: SparkContext, dataPath: String, date: String) {
    sc.textFile(dataPath) flatMap {line =>
      val Array(_, follow) = line.split("\t")
      val obj = JSON.parseObject(follow)
      if(obj.containsKey("target") && obj.containsKey("status") && obj.getInteger("status") == 1)
        Array[(String, Int)]((obj.getString("target"), 1))
      else Array.empty[(String, Int)]
    } reduceByKey((x, y) => x + y) foreachPartition { part =>
      val oneDayBefore = DateUtil.getOneDayBefore(DateUtil.parseDate(date))
      part.foreach { case (id, cnt) =>
        val jedisClient = new PipelinedJedisClient
        jedisClient.send(s"${followKeyPrefix}_${id}_$oneDayBefore",
                         s"${followKeyPrefix}_${id}_$date", cnt)
      }
    }
  }

  /**
    * 从es的customer_touring计算房源当日的带看量
    */
  def accDailyHouseTouring(sc: SparkContext, dataPath: String, date: String) {
    sc.textFile(dataPath) flatMap {line =>
      val Array(_, follow) = line.split("\t")
      val obj = JSON.parseObject(follow)
      if(obj.containsKey("house_id") && obj.containsKey("creation_date")
        && obj.getString("creation_date").substring(0, 8) == date) Array[(String, Int)]((obj.getString("house_id"), 1))
      else Array.empty[(String, Int)]
    } reduceByKey((x, y) => x + y) foreachPartition { part =>
      val oneDayBefore = DateUtil.getOneDayBefore(DateUtil.parseDate(date))
      part.foreach { case (id, cnt) =>
        val jedisClient = new PipelinedJedisClient
        jedisClient.send(s"${touringKeyPrefix}_${id}_$oneDayBefore",
                         s"${touringKeyPrefix}_${id}_$date", cnt)
      }
    }
  }
}
