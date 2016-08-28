package com.lianjia.profiling.common.elasticsearch

import java.util

/**
  * @author fenglei@lianjia.com on 2016-04
  */
object Types {
  type Document = Map[String, AnyRef]
  type Field = String
  type Value = AnyRef
  type Params = Map[String, Value]
  type Doc = Map[Field, Value]
  type JMap[K, V] = util.HashMap[K, V]
  type JLong = java.lang.Long
  type JDocument = util.Map[String, Object]
}
