package com.lianjia.profiling.stream

import com.lianjia.profiling.common.elasticsearch.ESClient
import com.lianjia.profiling.common.elasticsearch.Types._
import kafka.common.TopicAndPartition
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object GetEsOffset extends App with ESClient {
  val esClient = getClient

  set()
  // get()

  def set() {
    val bulk = esClient.prepareBulk()
    val doc = new JMap[String, Object]()
    doc.put("topic", "test8899")
    doc.put("partition", new Integer(2))
    doc.put("offset", new java.lang.Long(1010129288338L))
    bulk.add(new UpdateRequest("stream_offset", "test8899", "0").doc(doc).docAsUpsert(true))
    val resp = bulk.get()
    println()
  }

  def get() = {
    val idx = "stream_offset"
    var streamOffsets = Map.empty[TopicAndPartition, Long]
    Array(/*"lianjiaApp-0", */ "test8899") foreach { topic =>
      var resp: SearchResponse = null
      do {
        try {
          resp = esClient.prepareSearch().setIndices(idx).setTypes(topic).get()
        } catch {
          case _: Throwable =>
            Thread.sleep(10)
        }
      } while (resp == null)

      resp.getHits.hits().foreach { hit =>
        val partition = hit.getFields.get("offset").getValue.asInstanceOf[Long]
        streamOffsets = streamOffsets.updated(TopicAndPartition(topic, 1), partition)
      }
    }
    streamOffsets
  }

}
