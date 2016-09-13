package com.lianjia.profiling.tool

import com.lianjia.profiling.common.hive.Dependencies
import com.lianjia.profiling.common.redis.PipelinedJedisClient
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object DailyImportHouseInfo {
  val logger = LoggerFactory.getLogger(DailyImportHouseInfo.getClass)

  val conf = new SparkConf().setAppName("profiling-daily-import-house-info")
  if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")

  val sc = new SparkContext(conf)
  val hiveCtx = new HiveContext(sc)

  def main(args: Array[String]) {
    val date = args(0)

    while (!Dependencies.checkPartitionExistence("data_center", "dim_merge_house_day", date)) {
      logger.info("sleeping until data_center.dim_merge_house_day is ready, date: " + date)
      Thread.sleep(10 * 60 * 1000)
    }

    val data = hiveCtx.sql(s"select * from data_center.dim_merge_house_day where pt = '${date}000000'").select("house_pkid", "city_id", "resblock_id")
               .map(_.mkString("\t"))

    data coalesce (1, false) foreachPartition { part =>
      val redisClient = new PipelinedJedisClient
      var cnt = 0
      for (line <- part) {
        val parts = line.split("\t")
        redisClient.kvSend(s"info_${parts(0)}", s"${parts(1)}\t${parts(2)}")
        cnt += 1
      }

      redisClient.kvFlush()
    }

    sc.stop()
  }
}
