package com.lianjia.profiling.stream

import java.net.InetAddress

import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object ScriptUpdateReqBuilderTest extends App {

  val client = {
    val settings = Settings.settingsBuilder().put("cluster.name", "my-application").build()
    TransportClient.builder.settings(settings).build.
    addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.30.17.2"), 9300))
  }

//  val reqs = RequestBuilder.newReq("user", "user", "AVOetNYx2LpYwtzNLmI_666991")
//             .addFieldScriptedUpdates(Map("mobile_durations" -> Seq(Map("duration" -> "99"))))
//             //.addUpdateReq(Map("user/user/AVOetNYx2LpYwtzNLmI_666991/user_id" -> Seq(Map("duration" -> "99"))))
//             .get()

  val bulk = client.prepareBulk()
  // reqs.foreach(bulk.add)

  val bulkResponse: BulkResponse = bulk.get()
  if (bulkResponse.hasFailures) {
    for(resp <- bulkResponse.getItems) {
      if(resp.isFailed) {
        println(resp.getFailureMessage)
      }
    }
  }
}
