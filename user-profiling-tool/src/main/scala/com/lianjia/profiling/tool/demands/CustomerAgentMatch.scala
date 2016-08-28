package com.lianjia.profiling.tool.demands

import com.lianjia.profiling.util.DateUtil
import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormat

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object CustomerAgentMatch {
  val sqlContext: SQLContext = null

  // hadoop fs -rmr /user/bigdata/profiling/*_customer_agent_*

  val dateStart = "20160701"
  val dateEnd = "20160724"
  val pt = s"${dateEnd}000000"

  var touringData = sqlContext.sql(
    s"""select
        |del.phone_a,
        |touring_house.created_code,
        |touring_house.hdic_house_id
        |from data_center.dim_merge_showing_house_day as touring_house
        |join data_center.dim_merge_showing_day as touring
        |on touring_house.showing_id = touring.id
        |and touring_house.app_id = touring.app_id
        |and touring_house.biz_type = 200200000001
        |and touring_house.city_id = 110000
        |and touring_house.created_time between '${DateTimeFormat.forPattern("yyyy-MM-dd").print(DateUtil.parseDate(dateStart))} 00:00:00'
        |and '${DateTimeFormat.forPattern("yyyy-MM-dd").print(DateUtil.parseDate(dateEnd).plusDays(1))} 00:00:00'
        |and touring.pt = '$pt'
        |and touring_house.pt = '$pt'
        |join data_center.dim_merge_custdel_day as del
        |on touring.cust_pkid = del.cust_pkid
        |and touring.app_id = del.app_id
        |and del.pt = '$pt'
        | """.stripMargin)


  touringData map { row =>
    val fields = row.toSeq
    (fields.head, fields.tail)
  } groupByKey() map {
    case (phone, tourings) =>
      s"$phone\t${tourings.map { touring => touring.mkString(":") }.mkString(",")}\t${tourings.size}"
  } saveAsTextFile s"/user/bigdata/profiling/touring_customer_agent_${dateStart}_${dateEnd}"

  def count(seq: Seq[String], tag: String): String = {
    var map = Map.empty[String, Int]
    seq.foreach { agent =>
      if (map.contains(agent)) map = map.updated(agent, map.get(agent).get + 1)
      else map = map.updated(agent, 1)
                }
    map.map { case (x, y) => s"$x:$y:$tag" }.mkString("\t")
  }

  touringData map { row =>
    val fields = row.toSeq
    (fields.head, fields.tail)
  } groupByKey() map {
    case (phone, tourings) =>
      val stat = count(tourings.map(_.head.toString).toSeq, "showing")
      s"$phone\t$stat"
  } coalesce (1, false) saveAsTextFile s"/user/bigdata/profiling/touring_customer_agent_ver2_${dateStart}_${dateEnd}"

  val contractData = sqlContext.sql(s"""select del.phone_a,
                                      |contr.created_code,
                                      |contr.hdic_house_id
                                      |from (select case length(cust_pkid)
                                      |                 when 10 then concat('5010', substr(cust_pkid, -8))
                                      |                 when 11 then concat('501', substr(cust_pkid, -9))
                                      |                 when 12 then concat('50', substr(cust_pkid, -10))
                                      |                 else null
                                      |             end as cust_pkid,
                                      |             hdic_house_id,
                                      |             deal_time,
                                      |             created_code,
                                      |             biz_type,
                                      |             city_id
                                      |      from data_center.dim_merge_contract_day ct
                                      |      where ct.pt = '$pt') contr
                                      |join data_center.dim_merge_custdel_day as del
                                      |on contr.cust_pkid = del.cust_pkid
                                      |and contr.biz_type = 200200000001
                                      |and contr.city_id = 110000
                                      |and contr.deal_time between '$dateStart' and '$dateEnd'
                                      |and del.pt = '$pt'""".stripMargin)

  contractData map { row =>
    val fields = row.toSeq
    (fields.head, fields.tail)
  } groupByKey() map {
    case (phone, contracts) =>
      s"$phone\t${contracts.map { touring => touring.mkString(":") }.mkString(",")}\t${contracts.size}"
  } saveAsTextFile s"/user/bigdata/profiling/contract_customer_agent_${dateStart}_${dateEnd}"

  contractData map { row =>
    val fields = row.toSeq
    (fields.head, fields.tail)
  } groupByKey() map {
    case (phone, contracts) =>
      val stat = count(contracts.map(_.head.toString).toSeq, "contract")
      s"$phone\t$stat"
  } coalesce (1, false) saveAsTextFile s"/user/bigdata/profiling/contract_customer_agent_ver2_${dateStart}_${dateEnd}"

}
