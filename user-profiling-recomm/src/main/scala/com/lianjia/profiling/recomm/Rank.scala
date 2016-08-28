package com.lianjia.profiling.recomm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object Rank {

  trait CombineHelper[K, V] {
    def combineValues(): RDD[(K, Vector[V])]
  }

  implicit def RDDView[K, V](self: RDD[(K, V)]): CombineHelper[K, V] = new CombineHelper[K, V] {
    def combineValues(): RDD[(K, Vector[V])] = {
      self.combineByKey((x: V) => Vector(x),
                        (c: Vector[V], x: V) => c :+ x,
                        (c1: Vector[V], c2: Vector[V]) => c1 ++ c2,
                        ItemBasedCF.partitionNum)
    }
  }

  def innerProd(itemList: Array[String], itemSimList: Iterable[(String, Float)]) = {
    var sum = 0.0F
    val itemSet = HashSet(itemList: _*)
    for((itemI, similarity) <- itemSimList) {
      if(itemSet.contains(itemI)) sum += similarity
    }
    sum
  }

  /**
    * matrix A: (user x, list(item i, 1(implicit)))
    * matrix B: (item i, list(item j, similarity))
    * Production: (user, list(item i, rank))
    *
    */
  def matrixMultiply(A: RDD[(String, Array[String])], B: RDD[(String, Array[(String, Float)])]) = {
    // 不需要shuffle, 应该是两个RDD partition-wise做笛卡尔积
    A.cartesian(B) map { case ((user, items), (itemI, similarities)) =>
      (user, (itemI, innerProd(items, similarities)))
    }
  }

  def compute(sc: SparkContext, userInterestPath: String, itemSimilarityPath: String, rankPath: String) = {
    val A = sc.textFile(userInterestPath) map { line =>
      val parts = line.split("\t")
      (parts.head, parts.tail)
    }

    val B = sc.textFile(itemSimilarityPath) map { line =>
      val (itemI: String, itemJ: String, similarity: String) = line.split("\t")
      (itemI, (itemJ, similarity.toFloat))
    } combineValues() map { case (x, y) => (x, y.toArray) }

    matrixMultiply(A, B) combineValues() map { case (user, candidates: Iterable[(String, Float)]) =>
      val sorted = candidates.toList.sortWith(_._2 > _._2) map { case (item, rank) =>
        s"($item,$rank)"
      } mkString("[", ",", "]")
      s"$user\t$sorted"
    } saveAsTextFile rankPath
  }
}
