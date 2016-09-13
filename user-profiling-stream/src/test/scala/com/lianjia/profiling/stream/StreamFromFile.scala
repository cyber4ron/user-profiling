package com.lianjia.profiling.stream

import java.util

import com.lianjia.profiling.common.hbase.client.BlockingBatchWriteHelper
import com.lianjia.profiling.common.{BlockingBackoffRetryProxy, Logging, RequestBuilder}
import com.lianjia.profiling.config.Constants
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder.{EventDoc, Doc}
import com.lianjia.profiling.stream.parser.OnlineUserMessageParser
import org.apache.hadoop.hbase.client.Row

import scala.io.Source

/**
  * @author fenglei@lianjia.com on 2016-04
  */

object StreamFromFile extends App with Logging {
  val logger = getLogger(classOf[Stream].getName)
  val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
  val hbaseProxy = new BlockingBatchWriteHelper("tmp:tbl1")

  var seq = 1L
  Source.fromFile("user-profiling-stream/eval_house").getLines().foreach { orgLine =>
    try {
      val line = orgLine.substring(4, orgLine.indexOf(", offset="))
      val users = new util.ArrayList[Doc]()
      val events = new util.ArrayList[EventDoc]()
      val redisKVs = new util.ArrayList[Array[AnyRef]]()
      val mutations = new util.ArrayList[Row]()
      val eventIdx = new util.ArrayList[Row]()
      OnlineUserMessageParser.parse(line, users, events, mutations, eventIdx, redisKVs, Constants.ONLINE_USER_IDX) // todo online_user

      val reqs = RequestBuilder.newReq()
      // send es docs
      for (i <- 0 until users.size()) {
        println(users.toString)
        reqs.setIdentity(users.get(i).idx, users.get(i).idxType, users.get(i).id).addUpsertReq(users.get(i).doc)
      }
      for (i <- 0 until events.size()) {
        println(events.toString)
        reqs.setIdentity(events.get(i).idx, events.get(i).idxType, events.get(i).id)
        .addUpsertReq(events.get(i).doc)
      }
      reqs.get().foreach(esProxy.send)
      esProxy.flush()

      // send hbase puts
      for (i <- 0 until mutations.size()) {
        println(mutations.get(i).toString)
        hbaseProxy.send(mutations.get(i))
      }
      hbaseProxy.flush()
      seq += 1
      if (seq % 100 == 0) logger.info(s"seq: $seq sent.")
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }
}
