package com.lianjia.profiling.batch

import com.lianjia.profiling.batch.hive.HqlFactory
import com.lianjia.profiling.batch.spark.BatchBase

/**
  * @author fenglei@lianjia.com on 2016-03.
  */
object HiveTest extends App with BatchBase {
//  val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
//
//  // Queries are expressed in HiveQL
//  HqlFactory.getDelegation("20160318").foreach { hql =>
//    val data = hiveCtx.sql(hql).map(_.toString()) // todo 保留row?
//    parseAndIndex(data, "custumercustomer/delegations", LineParser.parseDel)

//    println(.count())
//    hiveCtx.setConf("", "")
//    val hvieC = hiveCtx.newSession()
//    // hvieC.setConf()
//  }

  println(HqlFactory.getDelegation("20160822"))
}
