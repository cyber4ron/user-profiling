package com.lianjia.profiling.common.elasticsearch

import com.lianjia.profiling.util.Properties
import org.elasticsearch.client.Client

import scala.collection.JavaConverters._

trait ESClient {
  def getClient(conf: Map[String, String]): Client = {
    if (conf.contains("spark.es.cluster.nodes") && conf.contains("spark.es.cluster.name")) {
      val nodes = conf.get("spark.es.cluster.nodes").get.split(",")
      getClient(conf.get("spark.es.cluster.name").get, nodes)
    } else {
      System.err.println("warn: missing es conf fields. use default.")
      getClient
    }
  }

  def getClient: Client = {
    val nodes = Properties.get("es.cluster.nodes").split(',')
    getClient(Properties.get("es.cluster.name"), nodes)
  }

  /*private*/ def getClient(clusterName: String, nodes: Array[String]) = {
    ClientFactory.getClient(clusterName, nodes.toList.asJava)
  }
}
