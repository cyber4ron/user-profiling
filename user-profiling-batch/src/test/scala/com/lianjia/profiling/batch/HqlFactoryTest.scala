package com.lianjia.profiling.batch

import com.lianjia.profiling.batch.hive.HqlFactory

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object HqlFactoryTest extends App {
  //  println("create table profiling.delegation20160616 as")
  //  println(HqlFactory.getDelegation("20160616"))
  //
  //  println("create table profiling.touring20160616 as")
  //  println(HqlFactory.getTouring("20160616"))
  //
  // println("create table profiling.touring_house20160601 as")
  // println(HqlFactory.getTouringHouse("20160601"))


  // println("create table profiling.contract20160712 as")
  println(HqlFactory. getContract("20160724"))

  //  println("create table profiling.house20160616 as")
  //  println(HqlFactory.getHouse("20160616"))
}
