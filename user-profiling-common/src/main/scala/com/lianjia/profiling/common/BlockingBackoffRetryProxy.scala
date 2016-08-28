package com.lianjia.profiling.common

import java.util.concurrent.atomic.AtomicInteger

import com.lianjia.data.profiling.log.Logger
import com.lianjia.profiling.common.elasticsearch.ESClient
import com.lianjia.profiling.common.retry.BoundedExponentialBackoffRetry
import com.lianjia.profiling.config.Constants
import com.lianjia.profiling.util.ExUtil
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue

import scala.collection.mutable.ArrayBuffer

/**
  * @author fenglei@lianjia.com on 2016-04
  */

private object BlockingBackoffRetryProxy extends ESClient with Logging {
  val bulkTimeout = TimeValue.timeValueMillis(5)
  val retryOnConflict = 10
  val NumThd = Constants.ES_BULK_REQUEST_SIZE_THD
  val esServerBusyTag = "nested: EsRejectedExecutionException"
  val versionConflictTag = "VersionConflictEngineException"
  val unavailableShardTag = "UnavailableShardsException"
  var acc = new AtomicInteger(0)
}

class BlockingBackoffRetryProxy(conf: Map[String, String]) extends Logging with ESClient with AutoCloseable {
  private lazy val backoffPolicy = new BoundedExponentialBackoffRetry

  import BlockingBackoffRetryProxy._
  System.err.print(s"conf in BlockingBackoffRetryProxy: $conf")
  private val logger = getLogger(classOf[BlockingBackoffRetryProxy].getName, conf)
  private val buffer = new ArrayBuffer[UpdateRequest](NumThd)
  private val client = getClient(conf)
  private var seq = 0L

  def getESClient = client

  def getBuf = buffer

  def send(req: UpdateRequest) = {
    buffer += req
    if (buffer.length >= NumThd) {
      logger.debug(s"proxy is flushing, seq: $seq")
      flush(logger, client)
      logger.debug(s"proxy flushing is done, seq: $seq")
      seq += 1
    }
  }

  def flush() {
    logger.info(s"proxy is flushing manually..., seq: $seq")
    flush(logger, client)
  }

  private def flush(logger: Logger, client: Client): Unit = try {
    if (buffer.isEmpty) {
      logger.debug(s"buffer is empty, returning...")
      return
    }

    val bulkReq = client.prepareBulk()
    buffer.foreach(r => bulkReq.add(r.retryOnConflict(retryOnConflict)))

    logger.debug(s"sending bulk request, seq: $seq...")
    buffer.headOption.foreach(x => logger.debug(s"seq: $seq, buffer(0).index/type: ${x.index()}/${x.`type`()}."))
    var resps = bulkReq.setTimeout(bulkTimeout).get()
    logger.debug(s"bulk request returned, seq: $seq, items: ${resps.getItems.length}")

    if (resps.hasFailures) {
      // copy loaded to current
      var current = new ArrayBuffer[UpdateRequest]
      for (x <- buffer) current.append(x)

      // log the failed req
      for (x <- resps.getItems) if (x.isFailed) {
        if (x.getFailure.getMessage.contains(esServerBusyTag)) {
          logger.info(s"request failed, seq: $seq, message: ${x.getFailure.getMessage}, " +
                        s"index/type: ${x.getIndex}/${x.getType}, docId: ${x.getId}, itemId: ${x.getItemId}, opType: ${x.getOpType}")
        } else {
          logger.warn(s"request failed, seq: $seq, message: ${x.getFailure.getMessage}, " +
                        s"stacktrace: ${ExUtil.getStackTrace(x.getFailure.getCause)}, " +
                        s"index/type: ${x.getIndex}/${x.getType}, docId: ${x.getId}, opType: ${x.getOpType}")
        }
      }

      // retry
      while (resps.getItems.exists(x => x.isFailed && (x.getFailure.getMessage.contains(esServerBusyTag)
        || x.getFailure.getMessage.contains(versionConflictTag)
        || x.getFailure.getMessage.contains(unavailableShardTag)))) {

        // put failed request to failed
        val failed = new ArrayBuffer[UpdateRequest]
        for (x <- resps.getItems) if (x.isFailed && (x.getFailure.getMessage.contains(esServerBusyTag)
          || x.getFailure.getMessage.contains(versionConflictTag)
          || x.getFailure.getMessage.contains(unavailableShardTag))) {
          failed.append(current(x.getItemId))
        }

        if (failed.isEmpty) throw new IllegalStateException("failed is empty while loop condition is satisfied.")

        // sleep
        val time = backoffPolicy.getSleepTimeMs
        logger.debug(s"part of bulk request failed, seq: $seq, retry times: ${backoffPolicy.getRetryTimes}, " +
                       s"failed(0).index/type: ${failed(0).index}/${failed(0).`type`()}, failed size: ${failed.size}, sleeping $time...")
        Thread.sleep(time)

        // do bulk request
        val bulkReq = client.prepareBulk()
        logger.debug(s"resending bulk request, seq: $seq, retry times: ${backoffPolicy.getRetryTimes}, size: ${bulkReq.numberOfActions}")
        for (x <- failed) bulkReq.add(x.retryOnConflict(retryOnConflict))
        resps = bulkReq.setTimeout(bulkTimeout).get()

        // prepare for next loop if failed again
        current = failed
      }
    } else {
      logger.debug(s"bulk request succeed, seq: $seq")
    }

    buffer.clear()
    backoffPolicy.reset()

  } catch {
    case ex: IllegalStateException =>
      logger.warn(s"IllegalStateException while flushing, ${ex.getMessage}")
    case ex: Exception =>
      logger.warn(s"Exception while flushing, ${ExUtil.getStackTrace(ex)}")
  }

  override def close(): Unit = {
    flush()
    client.close()
  }
}
