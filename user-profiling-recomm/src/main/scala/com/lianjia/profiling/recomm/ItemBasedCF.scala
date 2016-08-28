package com.lianjia.profiling.recomm

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object ItemBasedCF {

  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val dataPath = s"$hdfsUri/user/bigdata/profiling/recomm/tourings"
  val userInterestPath = s"$hdfsUri/user/bigdata/profiling/recomm/user_interest"
  val simPath = s"$hdfsUri/user/bigdata/profiling/recomm/house_similarity"
  val rankPath = s"$hdfsUri/user/bigdata/profiling/recomm/house_rank"

  var partitionNum = 16
  var maxItems = 200

  def main(args: Array[String]) {

    val conf = new SparkConf()/*.setAppName("house-based-CF")*/
    if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[1]")

    conf.set("spark.es.nodes", "10.10.35.14:9200,10.10.35.15:9200,10.10.35.16:9200")
    conf.set("spark.es.mapping.date.rich", "false")

    val sc = new SparkContext(conf)
    partitionNum = sc.getExecutorStorageStatus.length // todo: test

    var dumpData = false
    args.toList match {
      case "--dump" :: tail => dumpData = true
      case _ =>
    }

    def isPhoneValid(phone: String) = {
      !phone.equalsIgnoreCase("null") && phone != "00000000001" && phone.length == 11
    }

    def dump() {
      sc.esRDD("customer_touring/touring") filter { case (id, doc) =>
        doc.get("city_id").get.toString == "110000" &&
          doc.contains("phone") &&
          doc.contains("hdic_house_id") &&
          doc.contains("city_id") &&
        isPhoneValid(doc.get("phone").get.toString)
      } map {
        case (id, doc) => s"${doc.get("phone").get}\t${doc.get("hdic_house_id").get}"
      } saveAsTextFile dataPath
    }

    if (dumpData) dump()

    try {
      Similarity.compute(sc, dataPath, userInterestPath, simPath)
      Rank.compute(sc, userInterestPath, simPath, rankPath)
    } finally {
      sc.stop()
    }
  }
}
