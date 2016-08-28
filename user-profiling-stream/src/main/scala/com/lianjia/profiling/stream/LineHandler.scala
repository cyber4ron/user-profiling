package com.lianjia.profiling.stream

import java.util

import com.lianjia.profiling.common.elasticsearch.index.IndexRoller
import com.lianjia.profiling.common.hbase.client.BlockingBatchWriteHelper
import com.lianjia.profiling.common.{BlockingBackoffRetryProxy, RequestBuilder}
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder.{EventDoc, Doc}
import com.lianjia.profiling.stream.parser.HouseEvalMessageParser
import org.apache.hadoop.hbase.client.Row
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-05
  */
object LineHandler {
  def processOnlineUser(line: String,
                        esProxy: BlockingBackoffRetryProxy,
                        hbaseProxy: BlockingBatchWriteHelper,
                        hbaseIndexProxy: BlockingBatchWriteHelper,
                        indexListener: IndexRoller,
                        parse: (String, util.ArrayList[Doc], util.ArrayList[EventDoc], util.ArrayList[Row], util.ArrayList[Row], String) => Unit,
                        toES: Boolean = true,
                        toHBase: Boolean = true): util.ArrayList[Doc] = {

    val users = new util.ArrayList[Doc]()
    val events = new util.ArrayList[EventDoc]()
    val eventsHbase = new util.ArrayList[Row]()
    val eventsIndicesHbase = new util.ArrayList[Row]()

    parse(line, users, events, eventsHbase, eventsIndicesHbase, indexListener.getIndex)

    if(toES) {
      // send user docs
      val reqs = RequestBuilder.newReq()
      for (i <- 0 until users.size()) {
        reqs.setIdentity(users.get(i).idx, users.get(i).idxType, users.get(i).id)
        .addUpsertReq(users.get(i).doc)
      }

      // send event docs
      for (i <- 0 until events.size()) {
        reqs.setIdentity(events.get(i).idx, events.get(i).idxType, events.get(i).id)
        .addUpsertReq(events.get(i).doc)
      }
      reqs.get().foreach(esProxy.send)
    }

    if(toHBase) {
      // send hbase puts
      for (i <- 0 until eventsHbase.size()) {
        hbaseProxy.send(eventsHbase.get(i))
      }

      // "dual write" to secondary index
      for (i <- 0 until eventsIndicesHbase.size()) {
        hbaseIndexProxy.send(eventsIndicesHbase.get(i))
      }
    }

    users.addAll(events)
    users
  }

  def processHouseEval(line: String,
                       esProxy: BlockingBackoffRetryProxy) {
    val request = HouseEvalMessageParser.parse(line)
    if (request == null) return

    esProxy.send(new UpdateRequest("house_eval_20160627", "ft", request.doc.get("request_id").toString)
                 .docAsUpsert(true)
                 .doc(request.doc))
  }
}
