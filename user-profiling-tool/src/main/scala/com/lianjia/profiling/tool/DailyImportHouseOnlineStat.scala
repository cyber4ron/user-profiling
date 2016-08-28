package com.lianjia.profiling.tool

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.common.hive.Dependencies
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.update.UpdateRequest
import org.slf4j.LoggerFactory

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object DailyImportHouseOnlineStat extends App {
  val logger = LoggerFactory.getLogger(DailyImportHouseOnlineStat.getClass)

  val conf = new SparkConf().setAppName("profiling-daily-import-house-online-stat")
  if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")

  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  val date = args(0)

  while(!Dependencies.checkPartitionExistence("data_center", "hot_bd_rpt_housesrc_hotnum_untiltoday", date)) {
    logger.info("sleeping until data_center.hot_bd_rpt_housesrc_hotnum_untiltoday is ready, date: " + date)
    Thread.sleep(10 * 60 * 1000)
  }

  import sql.implicits._

  case class HouseStat(id: String, pvnum: Long, folnum: Long, shownum: Long)

  val data = sql.sql(s"select * from data_center.hot_bd_rpt_housesrc_hotnum_untiltoday where pt = '${date}000000'")
             .as[HouseStat]

  import scala.collection.JavaConverters._

  data.foreachPartition { part =>
    val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
    for(stat <- part) {
      esProxy.send(new UpdateRequest("online_house_stat", "stat", stat.id)
                   .docAsUpsert(true)
                   .doc(Map("pv" -> stat.pvnum.toInt,
                            "fl" -> stat.folnum.toInt,
                            "tr" -> stat.shownum.toInt,
                            "date" -> date.toInt).asJava))
    }

    esProxy.flush()
  }

  sc.stop()
}
