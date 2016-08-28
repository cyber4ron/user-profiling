package com.lianjia.profiling.stream

import com.lianjia.profiling.common.elasticsearch.ESClient
import org.elasticsearch.action.bulk.BulkResponse

import scala.io.Source

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object MessageParserTest extends ESClient {
  def main(args: Array[String]) {
    val client = getClient(Map.empty)
    for (line <- Source.fromFile("kafkaMessage").getLines()) {
      val reqs = MessageParserScala.parseKafkaMessage(line)

      val bulk = client.prepareBulk()
      reqs.foreach(bulk.add)

      val bulkResponse: BulkResponse = bulk.get()
      if (bulkResponse.hasFailures) {
        for (resp <- bulkResponse.getItems) {
          if (resp.isFailed) {
            println(resp.getFailureMessage)
          }
        }
      }
    }
  }
}
