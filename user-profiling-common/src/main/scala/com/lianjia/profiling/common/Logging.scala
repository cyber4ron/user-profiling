package com.lianjia.profiling.common

import com.lianjia.data.profiling.log.{Logger, LoggerFactory}
import com.lianjia.profiling.util.Properties

trait Logging {
  def getLogger(clazz: String, conf: Map[String, String] = Map()): Logger = {
    System.err.print(s"conf in Logging: $conf")
    if (conf.contains("spark.logging.enable") && conf.get("spark.logging.enable").get.equals("true")
      && conf.contains("spark.logging.host") && conf.contains("spark.logging.port")) {
      LoggerFactory.getBlockingLogger(clazz, conf.get("spark.logging.host").get,
                                      Integer.parseInt(conf.get("spark.logging.port").get))
    }
    else {
      LoggerFactory.getBlockingLogger(clazz, Properties.get("logging.host"),
                                      Integer.parseInt(Properties.get("logging.port")))
    }
  }
}
