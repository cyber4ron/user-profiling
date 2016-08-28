package com.lianjia.profiling.batch.hive

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

/**
  * 无解析,只append!
  * todo 定义些中间结构
  * todo 嵌套hql?
  */
object HqlBuilder {
  def newHql() = new HqlBuilder

  object OP extends Enumeration {
    val EQ = Value("=")
  }

}

class HqlBuilder private {
  private val sb = StringBuilder.newBuilder

  def set(prop: String, value: String) = {
    sb.append(s"set $prop=$value")
    this
  }

  def dropIfExist(db: String, tbl: String) = {
    sb.append(s"drop table if exists $db.$tbl")
    this
  }

  def createAs(db: String, tbl: String) = {
    sb.append(
      s"""clearAndCreate table $db.$tbl
          |row format delimited
          |fields terminated by '\t'
          |lines terminated by '\n' as
          |""".stripMargin)
    this
  }

  def select(fields: String*) = {
    sb.append("select\n")
    for (i <- 0 until fields.length - 1)
      sb.append(s"${fields(i)},\n")
    sb.append(s"${fields(fields.length - 1)}\n")
    this
  }

  def from(db: String, tbl: String) = {
    sb.append(s"from $db.$tbl\n")
    this
  }

  def from(db: String, tbl: String, alias: String) = {
    sb.append(s"from $db.$tbl as $alias\n")
    this
  }

  def where(operand1: String, operator: HqlBuilder.OP.Value, operand2: String) = {
    sb.append(s"where $operand1 $operator $operand2\n")
    this
  }

  def join(db: String, tbl: String, alias: String) = {
    sb.append(s"join $db.$tbl as $alias\n")
    this
  }

  def leftJoin(db: String, tbl: String, alias: String) = {
    sb.append(s"left join $db.$tbl as $alias\n")
    this
  }

  def on(operand1: String, operator: HqlBuilder.OP.Value, operand2: String) = {
    sb.append(s"on $operand1 $operator $operand2\n")
    this
  }

  def and(operand1: String, operator: HqlBuilder.OP.Value, operand2: String) = {
    sb.append(s"and $operand1 $operator $operand2\n")
    this
  }

  def hql() = sb.result()
}
