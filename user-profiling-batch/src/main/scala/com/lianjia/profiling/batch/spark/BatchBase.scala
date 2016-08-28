package com.lianjia.profiling.batch.spark

import com.lianjia.profiling.common.elasticsearch.ESClient
import com.lianjia.profiling.util.Arguments
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j

/**
  * @author fenglei@lianjia.com on 2016-04
  */
trait BatchBase extends Arguments with ESClient {

  @args4j.Option(name = "--cold-start")
  var isColdStart = false

  @args4j.Option(name = "--date")
  var date = "20160429"

  @args4j.Option(name = "--app-name")
  var appName = "spark-test"

  @args4j.Option(name = "--run-specified")
  var runSpecified = false

  @args4j.Option(name = "--delegation")
  var runDel = false

  @args4j.Option(name = "--touring")
  var runTouring = false

  @args4j.Option(name = "--contract")
  var runContract = false

  @args4j.Option(name = "--touring-house")
  var runTouringHouse = false

  @args4j.Option(name = "--house")
  var runHouse = false

  @args4j.Option(name = "--parallel")
  var isParallel = false

  protected def getSc = {
    if (System.getProperty("os.name") == "Mac OS X") {
      val conf = new SparkConf().setMaster("local[1]").setAppName("profiling-batch")
      new SparkContext(conf)
    }
    else {
      val conf = new SparkConf().setAppName("profiling-batch")
      new SparkContext(conf)
    }
  }
}
