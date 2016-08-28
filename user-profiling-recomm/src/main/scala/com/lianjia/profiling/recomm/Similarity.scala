package com.lianjia.profiling.recomm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object Similarity extends App {

  def count(sc: SparkContext, dataPath: String): RDD[(String, Int)] = {
    val userItemPair = sc.textFile(dataPath)
    userItemPair map { line => line.split("\t") } filter(_.size == 2) map { parts =>
      val (_, item: String) = parts
      (item, 1)
    } reduceByKey (_ + _)
  }

  def compute(sc: SparkContext, dataPath: String, userInterestPath: String, simPath: String) {
    val userItemPair = sc.textFile(dataPath)

    import Rank.RDDView

    val userInterest = userItemPair.map(_.split("\t")).filter(_.size == 2) map { parts =>
      (parts(0), parts(1))
    } combineValues() map { case (x, y) => (x, y.toArray) }

    userInterest.saveAsTextFile(userInterestPath)

    userInterest flatMap { case (user, items) =>
      if (items.length > ItemBasedCF.maxItems) List.empty[((String, String), Int)]
      else {
        for (x <- items; y <- items; if x < y) // todo: test
          yield ((x, y), 1)
      }
    } reduceByKey (_ + _) map { case ((x, y), num) =>
      (x, (y, num))
    } join count(sc, dataPath) map { case (x, ((y, num), xNum)) =>
      s"$x\t$y\t${num.toFloat / xNum}"
    } saveAsTextFile simPath
  }
}
