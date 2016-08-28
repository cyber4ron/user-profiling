package com.lianjia.data


/**
  * @author fenglei@lianjia.com on 2016-08
  */

object Pipeline {

/*  def main() {
  }

  val spark = {
    SparkSession.builder()
    .appName("Spark SQL")
    .master("local[2]")
    .getOrCreate()
  }

  def example() {

    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    val lr = new LogisticRegression()

    lr.setMaxIter(10)
    .setRegParam(0.01)

    val model1 = lr.fit(training)

    val paramMap = ParamMap(lr.maxIter -> 20)
                   .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
                   .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training, paramMapCombined)

    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    model2.transform(test)
    .select("features", "label", "myProbability", "prediction")
    .collect()
    .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
      println(s"($features, $label) -> prob=$prob, prediction=$prediction")
             }
  }

  def pipeline() {
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
                    .setInputCol("text")
                    .setOutputCol("words")
    val hashingTF = new HashingTF()
                    .setNumFeatures(1000)
                    .setInputCol(tokenizer.getOutputCol)
                    .setOutputCol("features")
    val lr = new LogisticRegression()
             .setMaxIter(10)
             .setRegParam(0.01)

    val pipeline = new Pipeline()
                   .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    model.transform(test)
    .select("id", "text", "probability", "prediction")
    .collect()
    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }
  }

  def sql = {
    case class Person(name: String, age: Long)

    import spark.implicits._
    spark.sqlContext.cacheTable("xx")

    val caseClassDS = Seq(Person("Andy", 32)).toDS().createOrReplaceTempView("viewXXX")
    val primitiveDS = Seq(1, 2, 3).toDS()

    val path = "examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]

    spark.sparkContext.makeRDD(Array()).toDS()

  }*/
}
