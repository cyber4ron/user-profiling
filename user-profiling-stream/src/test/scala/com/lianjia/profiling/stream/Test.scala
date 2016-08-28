package com.lianjia.profiling.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.immutable.HashMap

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
                     .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

object Test {
  def test() {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

  def main(args: Array[String]) {
    kafkaTest()
  }

  /**
    * todo .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(5), 2) ???
    */
  def kafkaTest(): Unit = {
    val bootstrapServers = "jx-lj-kafka01.vm.lianjia.com:9092,jx-lj-kafka02.vm.lianjia.com:9092,jx-lj-kafka03.vm.lianjia.com:9092"

    val conf = new SparkConf().setMaster("local[4]").setAppName("KafkaTest")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = KafkaUtils.createDirectStream[String,
      String,
      StringDecoder,
      StringDecoder](ssc, HashMap[String, String]("bootstrap.servers" -> bootstrapServers,
                                                  "group.id" -> "kafka-test-4"),
                     Set[String]("lianjiaApp")).map(_._2).filter(_.contains("[bigdata."))

    val parsed = lines.map(MessageParserScala.parseKafkaMessage)

    parsed.print()

    ////
    //    val stream1 = ssc.textFileStream("kafkaMessage").map(x => (x, x))
    //    val stream2 = ssc.textFileStream("kafkaMessage").map(x => (x, x))
    //
    //    val joined = stream1.join(stream2)
    //    println(joined.slideDuration) // 5000 ms
    //
    //    val stream3 = stream1.window(Seconds(20))
    //    val stream4 = stream2.window(Minutes(100))
    //
    //    val joined2 = stream3.join(stream4)
    //    println(stream3.slideDuration) // 5000 ms
    //    println(stream4.slideDuration) // 5000 ms
    //    println(joined2.slideDuration) // 5000 ms
    //
    //    joined2.print()

    ssc.checkpoint("profiling")
    ssc.start()
    ssc.awaitTermination()
  }
}
