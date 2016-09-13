package com.lianjia.profiling.tagging.batch

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.tagging.features.UserPreference
import com.lianjia.profiling.tagging.tag.UserTag
import com.lianjia.profiling.tagging.user.OnlineEventTagging
import com.lianjia.profiling.util.{DateUtil, ZipUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object PreferencePreComputing {

  val maxNoActionUserRetention = 90 * 86400 * 1000L // 90 days

  lazy val sc = {
    val conf = new SparkConf().setAppName("profiling-tag-pre-computing")
    if (System.getProperty("os.name") == "Mac OS X") conf.setMaster("local[2]")
    new SparkContext(conf)
  }

  def removePath(path: String) {
    val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUri), hadoopConf)

    try {
      println(s"removing $path")
      if(path.length < "/user/bigdata/profiling".length) {
        println(s"$path is to short!")
        return
      }
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch {
      case _: Throwable =>
        println(s"remove $path failed.")
    }
  }

  def getPreferFromEvents(id: String, events: Array[String]) = {
    import scala.collection.JavaConverters._
    type JMap = java.util.Map[String, AnyRef]

    val evtTypes = Set("dtl", "mob_dtl", "fl", "mob_fl")

    val eventsAsMap = (events flatMap { evt: String =>
      val obj = JSON.parseObject(evt)
      if(evtTypes.contains(obj.get("evt").toString))
        Array[JMap](obj.entrySet().asScala.map { case entry =>
          (entry.getKey, entry.getValue)
        }.toMap.asJava)
      else Array.empty[JMap]
    }).toList.asJava

    if(eventsAsMap.size() > 0) {
      val prefer = OnlineEventTagging.compute(if (eventsAsMap.asScala.exists(x => x.containsKey("ucid"))) UserTag.UCID
                                              else UserTag.UUID, id, eventsAsMap)
      Array[(String, UserPreference)]((id, prefer))
    } else {
      Array.empty[(String, UserPreference)]
    }
  }

  def compute(data: RDD[(ImmutableBytesWritable, Result)], path: String, partitions: Int = 48) {
    import scala.collection.JavaConverters._

    val preference = data.map { case (k, v) =>
      (new String(k.get()), v.getColumnCells("evt".getBytes, "".getBytes).asScala
                            .toArray.map(x => new String(x.getValue)))
    } repartition partitions flatMap { case (id, evts) =>
      val events = evts flatMap { evt =>
        Array[String](new String(ZipUtil.inflate(evt)).split("\t"): _*)
      }

      getPreferFromEvents(id, events) map { case (user, prefer) =>
        (user, ZipUtil.deflate(prefer.serialize()))
      }
    }

    preference map { case (id, prefer) => s"$id\t$prefer" } saveAsTextFile path
  }

  def computeFromFile(data: RDD[(String, String)], path: String, partitions: Int = 48) {
    val preference = data repartition partitions flatMap { case (id, evts) =>
      val events = evts.split("\t") flatMap { evt =>
        Array[String](new String(ZipUtil.inflate(evt)).split("\t"): _*)
      }

      getPreferFromEvents(id, events) map { case (user, prefer) =>
        (user, ZipUtil.deflate(prefer.serialize()))
      }
    }

    preference map { case (id, prefer) => s"$id\t$prefer" } saveAsTextFile path
  }

  /**
    * e.g. table = "profiling:online_user_event_201608"
    * path = "/user/bigdata/profiling/xxx"
    */
  def backtraceFromHbase(sc: SparkContext, table: String, path: String) {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set(TableInputFormat.SCAN_MAXVERSIONS, "100")

    compute(sc.newAPIHadoopRDD(conf,
                               classOf[TableInputFormat],
                               classOf[ImmutableBytesWritable],
                               classOf[Result]), path)
  }

  def backtraceFromHDFS(sc: SparkContext, path: String) {
    val date = "20160906"
    val days = 6
    val dataPaths = (0 until days).map(i => DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(i)))
                    .map(dt => s"/user/bigdata/profiling/online_user_assoc_$dt")
    val data = sc.textFile(dataPaths.mkString(","))

    computeFromFile(data map { line =>
      val parts = line.split("\t")
      (parts(0), parts(1))
    }, path)
  }

  def dailyUpdate(sc: SparkContext, date: String) {

    val oneDayBefore = DateUtil.getOneDayBefore(DateUtil.parseDate(date))
    val oneWeekBefore = DateUtil.toFormattedDate(DateUtil.parseDate(date).minusDays(7))

    // compute daily inc prefer
    val associatedPath = s"/user/bigdata/profiling/online_user_assoc_$date"
    val mergedPreferPath = s"/user/bigdata/profiling/online_user_prefer_$date"
    val mergedIncPreferPath = s"/user/bigdata/profiling/online_user_prefer_inc_$date"
    val baseMergedPreferPath = s"/user/bigdata/profiling/online_user_prefer_$oneDayBefore"
    val oneWeekBeforeMergedPreferPath = s"/user/bigdata/profiling/online_user_prefer_$oneWeekBefore"
    // val preferTable = "profiling:online_user_prefer_v2"

    val inc = sc.textFile(associatedPath) flatMap { line =>
      val Array(id, base64) = line.split("\t")
      val events = new String(ZipUtil.inflate(base64)).split("\t")

      getPreferFromEvents(id, events)
    }

    // 从hbase表读base
    // val conf = HBaseConfiguration.create()
    // conf.set(TableInputFormat.INPUT_TABLE, preferTable)
    // conf.set(TableInputFormat.SCAN_MAXVERSIONS, "100")

    // val base = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    //                               classOf[ImmutableBytesWritable], classOf[Result]) map { case (k, v) =>
    //   val (id, ser) = (new String(k.get()), new String(v.value()))
    //   val prefer = UserPreference.deserialize(ZipUtil.inflate(ser))
    //   (id, prefer)
    // }

    // 从hdfs上读base
    val base = sc.textFile(baseMergedPreferPath) map { line =>
      val Array(id, ser) = line.split("\t")
      val prefer = UserPreference.deserialize(ZipUtil.inflate(ser))
      (id, prefer)
    }

    // merge inc & base prefer
    val merged = (inc fullOuterJoin base) flatMap { case (id, (incPrefer, basePrefer)) =>
      if (incPrefer.isDefined && basePrefer.isDefined)
        Array[(String, UserPreference)]((id, UserPreference.decayAndMerge(basePrefer.get, incPrefer.get)))
      else if (incPrefer.isDefined)
        Array[(String, UserPreference)]((id, incPrefer.get))
      else if (basePrefer.isDefined && System.currentTimeMillis() - basePrefer.get.getWriteTs > maxNoActionUserRetention)
        Array[(String, UserPreference)]((id, basePrefer.get))
      else Array.empty[(String, UserPreference)]
    }

    val mergedInc = (inc leftOuterJoin base) map { case (id, (incPrefer, basePrefer)) =>
      (id, if (basePrefer.isDefined) UserPreference.decayAndMerge(basePrefer.get, incPrefer)
      else incPrefer)
    }

    removePath(oneWeekBeforeMergedPreferPath)
    removePath(mergedPreferPath)
    removePath(mergedIncPreferPath)

    // save to hdfs
    mergedInc map { case (id, prefer) =>
      s"$id\t${ZipUtil.deflate(prefer.serialize())}"
    } saveAsTextFile mergedIncPreferPath

    merged map { case (id, prefer) =>
      s"$id\t${ZipUtil.deflate(prefer.serialize())}"
    } saveAsTextFile mergedPreferPath
  }

  def merge(basePath: String, incPath: String, mergedPath: String) {
    val basePrefer = sc.textFile(basePath) map { line =>
      val Array(id, ser) = line.split("\t")
      val prefer = UserPreference.deserialize(ZipUtil.inflate(ser))
      (id, prefer)
    }

    val incPrefer = sc.textFile(incPath) map { line =>
      val Array(id, ser) = line.split("\t")
      val prefer = UserPreference.deserialize(ZipUtil.inflate(ser))
      (id, prefer)
    }

    basePrefer fullOuterJoin incPrefer map { case (id, (base, inc)) =>
      if (base.isDefined && inc.isDefined) (id, UserPreference.decayAndMerge(base.get, inc.get))
      else if (base.isDefined) (id, base.get)
      else (id, inc.get)
    } map { case (id, prefer) =>
      s"$id\t${ZipUtil.deflate(prefer.serialize())}"
    } saveAsTextFile mergedPath
  }

  def main(args: Array[String]) {
    dailyUpdate(sc, args(0))
  }

}
