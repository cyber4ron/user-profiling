package com.lianjia.profiling.tool

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object Metric {

  def binaryMetric(labelAndPred: Array[(Double, Double)], threshold: Double) = {
    val precision = labelAndPred.count {
      case (l, p) =>
        val y = if(p > threshold) 1 else 0
        l == y
    }.toFloat / labelAndPred.length

    val recall = labelAndPred.count {
      case (l, p) =>
        val y = if(p > threshold) 1 else 0
        y == 1
    }.toFloat / labelAndPred.count(_._1 == 1)

    val fp = labelAndPred.count {
      case (l, p) =>
        val y = if(p > threshold) 1 else 0
        l == 0 && y == 1
    }.toFloat

    (precision, recall, fp)
  }

}
