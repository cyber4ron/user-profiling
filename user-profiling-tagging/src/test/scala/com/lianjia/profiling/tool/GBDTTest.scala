package com.lianjia.profiling.tool

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

object GBDTTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GradientBoostedTreesRegressionExample")
    if (System.getProperty("os.name") == "Mac OS X")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "user-profiling-tagging/src/test/resources/train.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 5 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 4
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error

    println("test data size: " + testData.count())

    val startMs = System.currentTimeMillis()
    val labelsAndPredictions = testData.map { point =>
      var prediction = model.predict(point.features)

      val label = if(prediction > 0.5) 1 else 0

      if(point.label != label) {
        // println("==>")
        // println(point.toString())
        // println(prediction)
      }

      if(point.label == label && label == 1 && point.features(74) == 0) println((point.label, prediction, label))

      (point.label, prediction, label, point)
    }
    println("ms:" + (System.currentTimeMillis() - startMs))

    val testMSE = labelsAndPredictions.map { case(v, p, l, _) => math.pow(v - p, 2)}.mean()
    val precision = labelsAndPredictions.filter{ case(v, p, l, _) => v == l }.count().toFloat / labelsAndPredictions.count()
    val recall = labelsAndPredictions.filter{ case(v, p, l, _) => v == 1 && l == 1 }.count().toFloat /
      labelsAndPredictions.filter{ case(v, p, l, _) => v == 1 }.count()
    val falsePositive = labelsAndPredictions.filter{ case(v, p, l, pt) => v == 0 && l == 1 && pt.features(74) > 0 }.count()

    val x = labelsAndPredictions.filter{ case(v, p, l, pt) => v == 0 && l == 1 && pt.features(74) > 0 }

    println(s"precision: $precision")
    println(s"recall: $recall")
    println(s"falsePositive: $falsePositive")

    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression GBT model:\n" + model.toDebugString)


    val metrics = testData.map { point =>
      var prediction = model.predict(point.features)
      (point.label, prediction)
    } collect

    val xx = Metric.binaryMetric(metrics, 0.9)



    // Save and load model
    model.save(sc, "target/tmp/myGradientBoostingRegressionModel")
    val sameModel = GradientBoostedTreesModel.load(sc, "target/tmp/myGradientBoostingRegressionModel")
    // $example off$
  }
}
