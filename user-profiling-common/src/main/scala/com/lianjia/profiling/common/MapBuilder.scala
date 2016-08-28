package com.lianjia.profiling.common

import com.lianjia.profiling.util.FieldUtil._
import com.lianjia.profiling.util.DateUtil._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object MapBuilder {
  def newMap: MapBuilder = {
    new MapBuilder
  }
}

class MapBuilder {
  private var map = Map.empty[String, AnyRef]

  def getDoc = map

  def addText(key: String, value: String): MapBuilder = {
    if (isFieldValid(value)) map += key -> value
    this
  }

  def addInt(key: String, value: String): MapBuilder = {
    val num = parseInt(value)
    if (num != null) map += key -> num
    this
  }

  def addIntIgnoreInvalid(key: String, value: String, default: Integer): MapBuilder = {
    val num = parseInt(value)
    if (num != null) map += key -> num
    else map += key -> default
    this
  }

  def addLong(key: String, value: String): MapBuilder = {
    val num = parseLong(value)
    if (num != null) map += key -> num
    this
  }

  def addDouble(key: String, value: String): MapBuilder = {
    val num = parseDouble(value)
    if (num != null) map += key -> num
    this
  }

  def addDateTime(key: String, value: String): MapBuilder = {
    val date = toDateTime(value)
    if (date != null) map += key -> date
    this
  }

  def addDateTime(key: String, ts: Long): MapBuilder = {
    val date = toDateTime(ts)
    if (date != null) map += key -> date
    this
  }

  def addDate(key: String, value: String): MapBuilder = {
    val date = toDate(value)
    if (date != null) map += key -> date
    this
  }
}
