package com.lianjia.profiling.tool.demands

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @author fenglei@lianjia.com on 2016-09
  */

object NewUserToDelegation {


  val conf = new SparkConf().setAppName("profiling-online-user-assoc")
  if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")
  val sc = new SparkContext(conf)

  val sqlContext = new HiveContext(sc)

  var date = "20160911"

  // 北京
  val delegationSql =
    s"""select
        |distinct(del.phone_a)
        |from data_center.dim_merge_custdel_day as del
        |where del.city_id = 110000
        |and del.created_time >= '2016-08-15 00:00:00' and del.created_time <= '2016-08-22 00:00:00'
        |and del.pt = '${date}000000'
        |""".stripMargin

  val delPhones = sqlContext.sql(delegationSql)
  delPhones persist StorageLevel.MEMORY_AND_DISK

  val phone_ucid = sc.textFile("/user/bigdata/profiling/ucid_phone_20160822").map { line =>
    val parts = line.split("\t")
    (parts(1), parts(0))
   }
  phone_ucid persist StorageLevel.MEMORY_AND_DISK

  val joined = delPhones.rdd.map(row => (row.get(0).toString, "")).join(phone_ucid).map { case (phone, (_, ucid)) => (ucid, phone)}

  val sample = joined.sample(false, 0.02, 0L) // 1154

  // 线上日志, 3个月 => ucid ts(最早)
  date = "20160815"
  val days = 31
  val dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(i)))
                  .map(dt => s"/user/bigdata/profiling/online_events_$dt")

  val parallel = Math.max(32, 512 * (days.toDouble / 31)).toInt

  val data = sc.textFile(dataPaths.mkString(","))

  // => (ucid, cnt)
  val tsData = data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      val obj = JSON.parseObject(parts(1))
      if (obj.containsKey("ucid") && obj.containsKey("ts"))
        Array[(String, Long)]((obj.getString("ucid"), obj.getLong("ts")))
      else Array.empty[(String, Long)]
    }
    else Array.empty[(String, Long)]
  } coalesce 512 reduceByKey { case (x, y) => math.min(x, y) }
  tsData persist StorageLevel.MEMORY_AND_DISK

  val result = tsData join joined map { case (ucid, (ts, phone)) => (ucid, ts)}

  val arr = result collect
  val diff = arr map { case (ucid, ts) => (ucid, (1471449600000L - ts) / 86400000) } sortBy { case (_, diff) => diff } filter { case (_, diff) => diff < 90
  } map { case (_, diff) => diff.toDouble }

  diff.sum / diff.size // => 20 day
}
