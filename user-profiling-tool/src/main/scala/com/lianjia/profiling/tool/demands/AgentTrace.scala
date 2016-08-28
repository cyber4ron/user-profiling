package com.lianjia.profiling.tool.demands

import com.lianjia.profiling.common.BlockingBackoffRetryProxy
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.sql.SQLContext
import org.elasticsearch.action.update.UpdateRequest

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object AgentTrace {
  val sqlContext: SQLContext = null

  // 轨迹
  var data = sqlContext.sql("select * from profiling.agent_trace_ext").map(_.mkString("\t"))

  import scala.collection.JavaConverters._

  data.map { line =>
    val Array(ucid, lon, lat, time, team_code, team_name, city_id, name) = line.split("\t")
    (ucid, (lon, lat, time, team_code, team_name, city_id, name))
           } foreachPartition { part =>
    val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
    part foreach {
      case (ucid, (lon, lat, time, teamCode, teamName, city_id, name)) =>
        val ts = DateUtil.parseDateTime(time).getMillis
        val id = math.abs(s"$ucid$lat$lon$teamCode$teamName$ts".hashCode).toString
        esProxy.send(new UpdateRequest("agent_trace", "trace", id)
                     .docAsUpsert(true)
                     .doc(Map("ucid" -> ucid,
                              "lat" -> lat.toFloat,
                              "lon" -> lon.toFloat,
                              "city_id" -> city_id,
                              "name" -> name,
                              "location" -> s"${lat.toString},${lon.toString}",
                              "team_code" -> teamCode,
                              "team_name" -> teamName,
                              "ts" -> ts).asJava))
    }
    esProxy.flush()
  }

  // 带看
  data = sqlContext.sql("select * from profiling.agent_house").map(_.mkString("\t"))

  data foreachPartition { part =>
    val esProxy = new BlockingBackoffRetryProxy(Map.empty[String, String])
    part foreach {
      case line =>
        val Array(ucid, hdic_house_id) = line.split("\t")
        val id = math.abs(s"$ucid$hdic_house_id".hashCode).toString
        esProxy.send(new UpdateRequest("agent_house", "house", id)
                     .docAsUpsert(true)
                     .doc(Map("ucid" -> ucid, "hdic_house_id" -> hdic_house_id).asJava))
    }
    esProxy.flush()
  }

}
