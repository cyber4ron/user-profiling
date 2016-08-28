package com.lianjia.profiling.stream

import com.lianjia.profiling.common.elasticsearch.ESClient

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object ParentChildTest extends App with ESClient {
  val client = getClient

  val resp = client.prepareUpdate("online_user", "search", "656565").setDoc("{}").setDocAsUpsert(true).setParent("da70ea64-a90a-4d52-bff3-f771ad3b5377").get()

  println()
}
