package com.lianjia.profiling.tool

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import org.apache.spark.SparkConf
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-05
  */

object LoadLocDemoData extends App {
  val proxy = new BlockingBackoffRetryProxy(new SparkConf().set("spark.es.cluster.name", "profiling")
                                            .set("spark.es.cluster.nodes", "10.10.35.14:9300")
                                            .getAll.toMap)

  scala.io.Source.fromFile("user-profiling-tool/src/main/resources/offline_user_demo_data").getLines() foreach { line =>
    val parts = line.split("\t")
    //    val doc: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    //    doc.put("phone", parts(0))
    //    doc.put("ucid", parts(1))
    //    doc.put("house_id", parts(2))
    //    doc.put("resblock_id", parts(3))
    //    doc.put("loc", s"${parts(4)},${parts(5)}")

    val doc =
      s"""
         |{"phone": "${parts(0)}", "ucid": "${parts(1)}", "house_id": "${parts(2)}", "resblock_id":"${parts(3)}", "loc":"${parts(5)},${parts(4)}"}
      """.stripMargin

    println(doc)

    val req = new UpdateRequest("touring_house_loc", "loc", parts(0) + parts(4) + parts(5))
              .docAsUpsert(true)
              .doc(doc)

    proxy.send(req)
  }

  proxy.close()
}
