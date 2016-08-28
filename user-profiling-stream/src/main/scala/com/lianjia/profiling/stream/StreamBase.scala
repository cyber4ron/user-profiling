package com.lianjia.profiling.stream


import com.lianjia.profiling.common.elasticsearch.ESClient
import com.lianjia.profiling.util.{Arguments, Properties}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.search.SearchResponse
import org.kohsuke.args4j

/**
  * @author fenglei@lianjia.com on 2016-04
  */

trait StreamBase extends Arguments with ESClient {

  @args4j.Option(name = "--app-name")
  var appName = "profiling-stream"
  @args4j.Option(name = "--batch-duration")
  var batchDuration: Int = 3

  var kafkaParams = Map.empty[String, String]

  protected def getKafkaVMStream(ssc: StreamingContext) = {
    kafkaParams = Map[String, String]("bootstrap.servers" -> "jx-lj-kafka01.vm.lianjia.com:9092,jx-lj-kafka02.vm.lianjia.com:9092,jx-lj-kafka03.vm.lianjia.com:9092",
                                      "auto.offset.reset" -> "smallest",
                                      "group.id" -> ssc.sparkContext.getConf.get("spark.kafka.groupId",
                                                                                 Properties.get("kafka.groupId")))
    KafkaUtils.createDirectStream[String,
      String,
      StringDecoder,
      StringDecoder](ssc,
                     kafkaParams,
                     Set[String]("house-eval-log"))
  }

  /**
    * https://spark.apache.org/docs/1.6.1/streaming-kafka-integration.html
    * 会用receiver, wal到hdfs, 太慢
    */
  protected def getStreamAlt(ssc: StreamingContext) = {
      KafkaUtils.createStream(ssc,
                              "jx-op-zk00.zeus.lianjia.com:2181,jx-op-zk01.zeus.lianjia.com:2181,jx-op-zk02.zeus.lianjia.com:2181/sysop",
                              ssc.sparkContext.getConf.get("spark.kafka.groupId", Properties.get("kafka.groupId")),
                              Properties.get("kafka.topic").split(",").map((_, 3)).toMap,
                              StorageLevel.MEMORY_AND_DISK_SER)
  }

  protected def getStream(ssc: StreamingContext) = {
    if (ssc.sparkContext.getConf.contains("spark.kafka.offset.from") && ssc.sparkContext.getConf.get("spark.kafka.offset.from") == "latest") {
      kafkaParams = Map[String, String]("bootstrap.servers" -> Properties.get("kafka.brokers"),
                                        "group.id" -> ssc.sparkContext.getConf.get("spark.kafka.groupId",
                                                                                   Properties.get("kafka.groupId")))
      KafkaUtils.createDirectStream[String,
        String,
        StringDecoder,
        StringDecoder](ssc,
                       kafkaParams,
                       Set[String](Properties.get("kafka.topic").split(","): _*))
    } else {
      kafkaParams = Map[String, String]("bootstrap.servers" -> Properties.get("kafka.brokers"),
                                        "group.id" -> ssc.sparkContext.getConf.get("spark.kafka.groupId",
                                                                                   Properties.get("kafka.groupId")))
      val idx = "stream_offset"
      var streamOffsets = Map.empty[TopicAndPartition, Long]
      val esClient = getClient

      var count = 0
      Properties.get("kafka.topic").split(",") foreach { topic =>
        var resp: SearchResponse = null
        do {
          try {
            count += 1
            resp = esClient.prepareSearch().setIndices(idx).setTypes(topic).get()
          } catch {
            case ex: Throwable =>
              ex.printStackTrace()
              Thread.sleep(100)
          }
        } while (resp == null && count < 10)

        if (resp != null) {
          resp.getHits.hits().foreach { hit =>
            val partition = hit.id().toInt
            val offset = hit.sourceAsMap().get("offset").toString.toLong
            streamOffsets = streamOffsets.updated(TopicAndPartition(topic, partition), offset)
          }
        }
      }

      // clearAndCreate direct stream. 见createDirectStream注释. ? todo
      if (streamOffsets.isEmpty) {
        kafkaParams += "auto.offset.reset" -> "earliest"
        KafkaUtils.createDirectStream[String,
          String,
          StringDecoder,
          StringDecoder](ssc,
                         kafkaParams,
                         Set[String](Properties.get("kafka.topic").split(","): _*))
      } else {
        KafkaUtils.createDirectStream[String,
          String,
          StringDecoder,
          StringDecoder,
          (String, String)](ssc,
                            kafkaParams,
                            streamOffsets,
                            (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
      }
    }
  }

  protected def getSc = {
    if (System.getProperty("os.name") == "Mac OS X") getLocalSc
    else {
      val conf = new SparkConf().setAppName(appName)
      new StreamingContext(conf, Seconds(batchDuration))

    }
  }

  protected def getLocalSc = {
    val conf = new SparkConf().setMaster("local[1]").setAppName(appName)
    new StreamingContext(conf, Seconds(batchDuration))
  }
}
