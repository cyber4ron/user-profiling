package com.lianjia.profiling.tool.demands

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fenglei@lianjia.com on 2016-08
  */

/**
  * 坐席需求, 用于挖掘商机
  */
object BOD extends App {

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

  val date = "20160823"

  val contractSql = s"""
                       |select del.phone_a,           -- 0
                       |contr.cott_pkid,              -- 1
                       |contr.cust_pkid,              -- 2
                       |contr.house_pkid,             -- 3
                       |contr.hdic_house_id,          -- 4
                       |contr.state,                  -- 5
                       |contr.deal_time,              -- 6
                       |round(contr.realmoney),       -- 7
                       |contr.created_uid,            -- 8
                       |contr.created_code,           -- 9
                       |contr.created_time,           -- 10
                       |contr.biz_type,               -- 11
                       |contr.city_id,                -- 12
                       |contr.app_id,                 -- 13
                       |house.district_code,          -- 14
                       |house.district_name,          -- 15
                       |house.bizcircle_code,         -- 16
                       |house.bizcircle_name,         -- 17
                       |house.resblock_id,            -- 18
                       |house.resblock_name,          -- 19
                       |house.room_cnt,               -- 20
                       |round(house.build_area),      -- 21
                       |house.signal_floor,           -- 22
                       |house.floor_name,             -- 23
                       |house.total_floor,            -- 24
                       |house.build_end_year,         -- 25
                       |house.is_sales_tax,           -- 26
                       |house.distance_metro_code,    -- 27
                       |house.distance_metro_name,    -- 28
                       |house.is_school_district,     -- 29
                       |house.is_unique_house,        -- 30
                       |house.face,                   -- 31
                       |round(house.total_prices)     -- 32
                       |from (select cott_pkid,
                       |             case length(cust_pkid)
                       |                 when 10 then concat('5010', substr(cust_pkid, -8))
                       |                 when 11 then concat('501', substr(cust_pkid, -9))
                       |                 when 12 then concat('50', substr(cust_pkid, -10))
                       |                 else null
                       |             end as cust_pkid,
                       |             house_pkid,
                       |             hdic_house_id,
                       |             state,
                       |             deal_time,
                       |             realmoney,
                       |             created_uid,
                       |             created_code,
                       |             created_time,
                       |             biz_type,
                       |             city_id,
                       |             app_id
                       |      from data_center.dim_merge_contract_day ct
                       |      where ct.pt = '${date}000000') contr
                       |join data_center.dim_merge_custdel_day as del
                       |on contr.cust_pkid = del.cust_pkid
                       |-- and contr.app_id = del.app_id
                       |and del.pt = '${date}000000'
                       |join data_center.dim_merge_house_day as house
                       |on substr(contr.house_pkid, -8) = substr(house.house_pkid, -8) and contr.hdic_house_id = house.hdic_house_id
                       |-- and contr.app_id = house.app_id
                       |and house.pt = '${date}000000'
                    """.stripMargin

  val delegationSql = s"""select
                          |del.phone_a,
                          |del.city_id
                          |from data_center.dim_merge_custdel_day as del
                          |where del.pt = '${date}000000'""".stripMargin

  val days = 31
  val dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(i)))
                  .map(dt => s"/user/bigdata/profiling/online_events_$dt")

  val parallel = Math.max(32, 512 * (days.toDouble / 31)).toInt

  val data = sc.textFile(dataPaths.mkString(","))

  val eventSet = Set("dtl", "fl", "mob_dtl", "mob_fl")

  val maxEvents = 1000
  val minActivities = 50
  val seq: (Seq[String], String) => Seq[String] = (seq: Seq[String], evt: String) => if(seq.length >= maxEvents) seq else seq :+ evt
  val comb: (Seq[String], Seq[String]) => Seq[String] = (x: Seq[String], y: Seq[String]) =>
    if(x.length >= maxEvents) x
    else if (y.length >= maxEvents) y
    else x ++ y

  val seqJson: (Seq[JSONObject], JSONObject) => Seq[JSONObject] = (seq: Seq[JSONObject], evt: JSONObject) => if(seq.length >= maxEvents) seq else seq :+ evt
  val combJson: (Seq[JSONObject], Seq[JSONObject]) => Seq[JSONObject] = (x: Seq[JSONObject], y: Seq[JSONObject]) =>
    if(x.length >= maxEvents) x
    else if (y.length >= maxEvents) y
    else x ++ y

  // 算UV
  val distinctUuids = data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        if (obj.containsKey("uuid")) {
          val id = obj.get("uuid").toString
          Array[String](id)
        }
        else Array.empty[String]
      }
    else Array[String]()
  } coalesce (parallel, false) distinct

  // 根据线上特定行为数量筛选ucid
  val ucids = (data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      if (!eventSet.contains(parts(0))) Array.empty[(String, String)]
      else {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        obj.put("evt", parts(0))
        if (obj.containsKey("ucid")) {
          val id = obj.get("ucid").toString
          Array[(String, String)]((id, JSON.toJSONString(obj, SerializerFeature.QuoteFieldNames)))
        }
        else Array.empty[(String, String)]
      }
    }
    else Array[(String, String)]()
  } coalesce (parallel, false)).aggregateByKey(Seq.empty[String])(seq, comb) flatMap { case (ucid, evts) =>
      val arr = evts.toArray
      if(arr.length > minActivities) {
        val idx = arr.indexWhere { evt =>
          val obj = JSON.parseObject(evt)
          if(obj.get("evt") == "fl") true
          else false
        }
        if (idx != -1) Array[String](ucid)
        else Array.empty[String]
      }
      else Array.empty[String]
  }

  ucids persist StorageLevel.MEMORY_AND_DISK

  val ucidsBJ = (data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      if (!eventSet.contains(parts(0))) Array.empty[String]
      else {
        val obj = JSON.parse(parts(1)).asInstanceOf[JSONObject]
        obj.put("evt", parts(0))
        if (obj.containsKey("ucid") && obj.containsKey("city_id") && obj.getString("city_id") == "110000") {
          val id = obj.get("ucid").toString
          Array[String](id)
        }
        else Array.empty[String]
      }
    }
    else Array[String]()
  } coalesce (parallel, false)).distinct()

  ucidsBJ persist StorageLevel.MEMORY_AND_DISK

  val bj = (ucidsBJ map ((_, ""))) join (ucids map ((_, "")))


  // 关联电话
  // sc.makeRDD(Array(("", Map.empty[String, AnyRef]))) map { case (_, map) =>
  //  s"${map.get("ucid").get}\t${map.get("phone").get}"
  //} saveAsTextFile "/user/bigdata/profiling/ucid_phone_20160822"

  val ucid_phone = sc.textFile("/user/bigdata/profiling/ucid_phone_20160822").map { line =>
    val parts = line.split("\t")
    (parts(0), parts(1))
  }

  val joined = (ucids map ((_, ""))) join(ucid_phone) map { case (ucid, (_, phone)) => (ucid, phone) }
  joined.persist(StorageLevel.MEMORY_AND_DISK)

  // 减去有委托的
  val delegationPhones = sqlContext.sql(delegationSql) map { row =>
    (row.getString(0), "")
  }

  val subDel = joined map { case (ucid, phone) =>
    (phone, ucid)
  } subtractByKey delegationPhones

  // 减去进一个月成交的
  val contractPhones = sqlContext.sql(contractSql).flatMap { row =>
    val parts = row.toSeq
    if (parts(10).toString.substring(0, 10) >= "2016-07-22") Array[(String, String)]((parts(0).toString, ""))
    else Array.empty[(String, String)]
  }

  contractPhones.persist(StorageLevel.MEMORY_AND_DISK)

  val subContr = subDel subtractByKey contractPhones // 8477

  // 减去黑名单的
  val blackListPhones = sc.textFile("/user/bigdata/profiling/other_company_broker_phones")
  val subBlackList = subContr subtractByKey blackListPhones.map((_, ""))
  subBlackList.persist(StorageLevel.MEMORY_AND_DISK)

  // 取 2000 sample
  val sample = sc.makeRDD(subBlackList.filter(_._2.startsWith("2")).takeSample(false, 2000, 0)) map { case (phone, ucid) => (ucid, phone) }

  val sampleBj = (ucidsBJ map ((_, ""))) join sample

  // 拿房屋信息
  val houses = sc.textFile("/user/bigdata/profiling/house_20160823") map {line =>
    val parts = line.split("\t")
    (parts(0), JSON.parseObject(parts(1)))
  }

  // 拿2000个ucid的登录信息
  val ucids_2000 = sc.textFile("/user/bigdata/profiling/ucids") map { ucid => (ucid, "") }
  val loginEvt = Set("usr")

  val xx = data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      if (!loginEvt.contains(parts(0))) Array.empty[(String, JSONObject)]
      else {
        val obj = JSON.parseObject(parts(1))
        obj.put("evt", parts(0))
        if (obj.containsKey("ucid")) {
          val ucid = obj.getString("ucid")
          Array[(String, JSONObject)]((ucid, obj))
        }
        else Array.empty[(String, JSONObject)]
      }
    }
    else Array.empty[(String, JSONObject)]
  } coalesce(parallel, false)

  val xx2 = xx map { case (ucid, _) => (ucid, 1) } reduceByKey { case (x, y) => x + y }

  xx persist StorageLevel.MEMORY_AND_DISK

  val xx4 = xx2 join ucids_2000 map { case (ucid, (cnt, _))  => (ucid, cnt) }
  val xx3 = xx2 join ucids_2000 map { case (ucid, (cnt, _))  => (ucid, cnt) }

  xx4 leftOuterJoin xx3 map { case (ucid, (cnt1, cnt2)) => s"$ucid\t$cnt1\t${cnt2.getOrElse(0)}"} saveAsTextFile "/user/bigdata/profiling/bod_2000_login_app_web"

  xx3 map { case (ucid, cnt) => s"$ucid\t$cnt" } saveAsTextFile "/user/bigdata/profiling/bod_2000_login_app"

  /*map { case (ucid, (obj, _)) =>

   }*/


  // 拿events
  val events = (data flatMap { line =>
    val parts = line.split("\t")
    if (parts.length == 2) {
      if(!eventSet.contains(parts(0))) Array.empty[(String, JSONObject)]
      else {
        val obj = JSON.parseObject(parts(1))
        obj.put("evt", parts(0))
        if (obj.containsKey("ucid")) {
          val ucid = obj.getString("ucid")
          Array[(String, JSONObject)]((ucid, obj))
        }
        else Array.empty[(String, JSONObject)]
      }
    }
    else Array.empty[(String, JSONObject)]
  } coalesce (parallel, false)).join(sample).flatMap { case (ucid, (evt, _)) =>
    val houseId = if(evt.containsKey("dtl_id")) evt.getString("dtl_id")
                  else if (evt.containsKey("fl_id")) evt.getString("fl_id")
                  else ""
    if(houseId == "") Array.empty[(String,JSONObject)]
    else Array[(String,JSONObject)]((houseId, evt))
  }.leftOuterJoin(houses).map { case (house_id, (evt, house)) =>
    if(house.isDefined) evt.put("house", house.get)
    (evt.getString("ucid"), evt)
  }.aggregateByKey(Seq.empty[JSONObject])(seqJson, combJson)

  events persist StorageLevel.MEMORY_AND_DISK

  removePath("/user/bigdata/profiling/bod_events_20160823")
  events map { case (ucid, evts) => s"$ucid\t${evts.map { x =>
    JSON.toJSONString(x, SerializerFeature.QuoteFieldNames) }.mkString("\t")}" } saveAsTextFile "/user/bigdata/profiling/bod_events_20160823"

  // 统计线上
  case class OnlineStat(ucid: String, pcDtl: Int, mobDtl: Int, fl: Int, lastFlTs: Long, flStat: Map[String, Int],
                        pcDtlStat: Map[String, Int], mobDtlStat: Map[String, Int])

  val onlineStat = events coalesce(32, false) map { case (ucid, events) =>
    var pcDtl = 0
    var mobDtl = 0
    var fl = 0
    var lastFlTs = 0L

    for(evt <- events) {
      evt.getString("evt") match {
        case "dtl" => pcDtl += 1
        case "mob_dtl" => mobDtl += 1
        case "fl" | "mob_fl" =>
          fl += 1
          lastFlTs = math.max(lastFlTs, evt.getLong("ts"))
      }
    }

    def getStat(seq: Seq[JSONObject]) = {
      seq map { evt =>
        val bizcircle = evt.getJSONObject("house").getString("bizcircle_name")
        val resblock = evt.getJSONObject("house").getString("resblock_name")
        (s"$bizcircle-$resblock", 1)
      } groupBy (_._1) map { case ((key, seq)) => (key, seq.size) }
    }

    val flStat = getStat(events.filter(x => x.getString("evt") == "fl" && x.containsKey("house")))
    val pcDtlStat = getStat(events.filter(x => x.getString("evt") == "dtl" && x.containsKey("house")))
    val mobDtlStat = getStat(events.filter(x => x.getString("evt") == "mob_dtl" && x.containsKey("house")))

    (ucid, OnlineStat(ucid, pcDtl, mobDtl, fl, lastFlTs, flStat, pcDtlStat, mobDtlStat))
  }

  onlineStat persist StorageLevel.MEMORY_AND_DISK_SER

  // 统计成交
  case class ContractInfo(phone: String, houseId: String, dealDate: String, price: Double)

  val contracts = sqlContext.sql(contractSql).map(_.mkString("\t")).flatMap { line =>
    val parts = line.split("\t")
    try {
      Array[(String, ContractInfo)]((parts(0), ContractInfo(parts(0), parts(3), parts(6), parts(7).toDouble)))
    } catch {
      case _ => Array.empty[(String, ContractInfo)]
    }
  } reduceByKey { case (x, y) =>
    if (x.dealDate > y.dealDate) x else y
  } join (sample map { case (ucid, phone) => (phone, ucid) }) map { case (phone, (contract, ucid)) =>
    (ucid, contract)
  }

  // 拿 ucid 注册时间
  case class UcidInfo(id: String, ctime: String)

  import sqlContext.implicits._

  val ucidInfo = sqlContext.sql("select id, ctime from data_center.dim_uc_user_day where pt = '20160823000000' ").as[UcidInfo].map { info =>
    (info.id, info.ctime)
  }.rdd.join(sample) map { case (ucid, (ctime, phone)) =>
    (ucid, ctime)
  }

  def mapToString(map: Map[String, Int]) = {
    map.toSeq.sortBy(_._2)(Ordering[Int].reverse) map { case (k, v) => Array(k, v).mkString("-") } mkString ";"
  }

  // merge, 这里已知关联的contracts为空
  onlineStat join ucidInfo join sample map { case (ucid, ((stat, ctime), phone)) =>
    Seq(ucid, phone, "", ctime, "", "", "", stat.fl, DateUtil.toApiDateTime(stat.lastFlTs), mapToString(stat.flStat),
        stat.pcDtl, mapToString(stat.pcDtlStat), stat.mobDtl, mapToString(stat.mobDtlStat)).mkString("\t")
  } saveAsTextFile "/user/bigdata/profiling/bod_2000_20160823"
}
