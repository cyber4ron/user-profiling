package com.lianjia.profiling.stream

import com.lianjia.profiling.common.BlockingBackoffRetryProxy

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object ESUpdateTest extends App {

  val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
  val client = esProxy.getESClient

  // 如果auto clearAndCreate index 和 dynamic mapping 都enable就可以
  val resp = esProxy.getESClient.prepareUpdate("x", "x", "x").setDoc("{}").setDocAsUpsert(true).get()

  Thread.sleep(1000 * 1000)
}
