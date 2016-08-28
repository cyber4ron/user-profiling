package com.lianjia.profiling.stream

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.stream.tool.LoadUcidMapping

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object UcidMappingTest extends App {

  val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
  LoadUcidMapping.processLine("2000000000000001\t1\t0\t189****6004\t\t18911746004\tNULL\tNULL\tNULL\tNULL\tNULL\t\t\t1\t\t0\t10.13.5.20\t2016-02-22 11:53:50\t2015-03-10 23:49:01\t2015-06-01 23:47:04\t20160501000000", esProxy)
  esProxy.close()
}
