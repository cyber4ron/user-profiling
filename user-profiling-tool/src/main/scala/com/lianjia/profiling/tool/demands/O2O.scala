package com.lianjia.profiling.tool.demands

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object O2O extends App {

  val conf = new SparkConf().setAppName("profiling-online-user-assoc")
  if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")
  val sc = new SparkContext(conf)

  val sqlContext = new HiveContext(sc)

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

  val date = "20160825"

  val onlineChannels = Set(
  "111400100100",
  "111400800100",
  "111400101800",
  "111400800900")

  val contractSql =
    s"""
       |select del.phone_a,
       |contr.deal_time
       |from (select cott_pkid,
       |             case length(cust_pkid)
       |                 when 10 then concat('5010', substr(cust_pkid, -8))
       |                 when 11 then concat('501', substr(cust_pkid, -9))
       |                 when 12 then concat('50', substr(cust_pkid, -10))
       |                 else null
       |             end as cust_pkid,
       |             deal_time
       |      from data_center.dim_merge_contract_day ct
       |      where city_id = 110000 and ct.pt = '${date}000000') contr
       |join data_center.dim_merge_custdel_day as del
       |on contr.cust_pkid = del.cust_pkid
       |and contr.deal_time >= '20160701' and contr.deal_time <= '20160801'
       |and del.pt = '${date}000000'
                    """.stripMargin

  // delegation with source
  val delegationSql =
    s"""select
        |del.phone_a,
        |min(cust.delegation_source_child)
        |from data_center.dim_merge_custdel_day as del
        |join data_center.t_cm_cust_basic as cust
        |on del.phone_a = cust.phone1
        |and del.city_id = 110000
        |and del.created_time >= '2016-07-01 00:00:00' and del.created_time <= '2016-08-01 00:00:00'
        |and del.pt = '${date}000000'
        |and cust.pt = '${date}000000'
        |group by del.phone_a
        |""".stripMargin

  val delegationCntSql =
    s"""select
        |del.phone_a,
        |count(*)
        |from data_center.dim_merge_custdel_day as del
        |where del.created_time >= '2016-06-01 00:00:00' and del.created_time <= '2016-08-01 00:00:00'
        |and city_id = 110000
        |and del.pt = '${date}000000'
        |group by del.phone_a
        |""".stripMargin

  // contr - phone
  val contract = sqlContext.sql(contractSql) map { row =>
    row.getString(0)
  } distinct()

  contract persist StorageLevel.MEMORY_AND_DISK

  // del - phone, src
  val delegation = sqlContext.sql(delegationSql) map {row =>
    (row.getString(0), row.getString(1))
  }

  val delegationCnt = sqlContext.sql(delegationCntSql) map {row =>
    (row.getString(0), row.getLong(1))
  }
  delegationCnt persist StorageLevel.MEMORY_AND_DISK

  val contrDelCnt = contract.map((_, "")) join delegationCnt map {
    case (phone, (_, cnt)) => (phone, cnt)
  }

  contrDelCnt persist StorageLevel.MEMORY_AND_DISK

  delegation persist StorageLevel.MEMORY_AND_DISK

  val delegationOffline = delegation filter { case (_, src) => !onlineChannels.contains(src) }

  val users = contract union ( delegation map { case (x, _) => x } )
  users.persist(StorageLevel.MEMORY_AND_DISK)

  // 关联ucid
  val phone_ucid = sc.textFile("/user/bigdata/profiling/ucid_phone_20160822").map { line =>
    val parts = line.split("\t")
    (parts(1), parts(0))
 }

  phone_ucid persist StorageLevel.MEMORY_AND_DISK

  val joined =  contract.map((_, "")).join(phone_ucid) map { case (phone, (ts, ucid)) => (ucid, phone) }
  // val joined =  delegationOffline.join(phone_ucid) map { case (phone, (_, ucid)) => (ucid, phone) }
  joined.persist(StorageLevel.MEMORY_AND_DISK)

  val days = 31
  val dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(i)))
                  .map(dt => s"/user/bigdata/profiling/online_events_$dt")

  val parallel = Math.max(32, 512 * (days.toDouble / 31)).toInt

  val data = sc.textFile(dataPaths.mkString(","))

  // => (ucid, cnt)
  val dtl = data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      val obj = JSON.parseObject(parts(1))
      if (obj.containsKey("ucid") && (parts(0) == "dtl" ||  parts(0) == "mob_dtl"))
        Array[String](obj.getString("ucid"))
      else Array.empty[String]
    }
    else Array.empty[String]
  } coalesce 64 /*distinct() */map ((_, 1)) reduceByKey { case (x, y) => x + y }

  dtl.persist(StorageLevel.MEMORY_AND_DISK)

  val onlineCnt = joined.join(dtl) map { case (ucid, (phone, cnt)) =>
    (phone, cnt)
  }

  contrDelCnt leftOuterJoin onlineCnt map {
    case (phone, (delCnt, dtlCnt)) => s"$phone\t$delCnt\t${dtlCnt.getOrElse(0)}"
  } saveAsTextFile "/user/bigdata/profiling/haichao4"

  // conversion map {case (a, b) => s"$a\t$b"} saveAsTextFile "/user/bigdata/profiling/haichao_online_channel"
  // users map {case (a, b) => s"$a"} saveAsTextFile "/user/bigdata/profiling/haichao_users"
}
