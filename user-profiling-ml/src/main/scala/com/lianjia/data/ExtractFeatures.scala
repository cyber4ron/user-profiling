package com.lianjia.data

import com.alibaba.fastjson.JSON
import com.lianjia.profiling.util.DateUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source
import scala.util.Random

/**
  * @author fenglei@lianjia.com on 2016-08
  */

object ExtractFeatures {

  val priceRanges = Array(0, 100, 150, 200, 250, 300, 400, 500, 800, 1000, 10000)
  val areaRanges = Array(0, 50, 80, 110, 140, 170, 200, 300, 1000)
  val buildingAgeRanges = Array(0, 5, 10, 20, 50)
  val dateSlot = 3 * 86400000

  case class Loc(val lon: Float, val lat: Float) {
    override def equals(o: Any) = o match {
      case that: Loc => math.abs(this.lon * 1000 - that.lon * 1000) + math.abs(this.lat * 1000 - that.lat * 1000) <= 10
      case _ => false
    }
    override def hashCode = lon.hashCode ^ lat.hashCode
  }

  // val dir = "user-profiling-tagging/src/main/resources/"
  val dir = "/home/work/fl/"

  var idx = -1
  val cities = Source.fromFile(dir + "cities").getLines.map(line => {
    idx += 1; line.split("\t")(0) -> idx
  }).toMap

  idx = -1
  val bizCircles = Source.fromFile(dir + "bizcircles").getLines.map(line => {
    idx += 1; line.split("\t")(0) -> idx
  }).toMap

  idx = -1
  val districts = Source.fromFile(dir + "districts").getLines.map(line => {
    idx += 1; line.split("\t")(0) -> idx
  }).toMap

  idx = -1
  val orients = Source.fromFile(dir + "orients").getLines.map(line => {
    idx += 1; line.split("\t")(0) -> idx
  }).toMap

  idx = -1
  val usages = Source.fromFile(dir + "house_usages").getLines.map(line => {
    idx += 1; line.split("\t")(0) -> idx
  }).toMap

  def createFeature() = {
    Map[String, AnyRef]("price" -> Array.ofDim[Int](priceRanges.length - 1),
                        "area" -> Array.ofDim[Int](areaRanges.length - 1),
                        "bldAge" -> Array.ofDim[Int](buildingAgeRanges.length - 1),
                        "room" -> Array.ofDim[Int](5),
                        "isUnq" -> Array.ofDim[Int](2),
                        "isSch" -> Array.ofDim[Int](2),
                        "isMetro" -> Array.ofDim[Int](2),
                        "orient" -> mutable.Map.empty[Int, Int],
                        "city" -> mutable.Map.empty[Int, Int],
                        "district" -> mutable.Map.empty[Int, Int],
                        "bizcircle" -> mutable.Map.empty[Int, Int],
                        "active" -> mutable.Map.empty[Int, Int],
                        "ips" -> mutable.Set.empty[(Byte, Byte, Byte, Byte)],
                        "locations" -> mutable.Set.empty[Loc]
    )
  }

  def updateIdFeats(map: Map[String, Int], id: String, vec: mutable.Map[Int, Int]) {
    if (map.contains(id)) {
      val idx = map.get(id).get
      if(vec.contains(idx)) {
        vec.put(idx, vec.get(idx).get + 1)
      } else {
        vec.put(idx, 1)
      }
    }
  }

  def updateCity(map: Map[String, Int], id: String, vec: mutable.Map[Int, Int]) {
    updateIdFeats(map, id, vec)
  }

  def updateDistrict(map: Map[String, Int], code: String, vec: mutable.Map[Int, Int]) {
    updateIdFeats(map, code, vec)
  }

  def updateBizCircle(map: Map[String, Int], code: String, vec: mutable.Map[Int, Int]) {
    updateIdFeats(map, code, vec)
  }

  /**
    * 忽略city差异
    */
  def updatePrice(price: Double, vec: Array[Int]): Array[Int] = {
    val pr = price / 10000
    if(pr < 30) return vec
    var idx = 1
    while(idx < priceRanges.length) {
      if(pr >= priceRanges(idx - 1) && pr < priceRanges(idx)) {
        vec(idx - 1) += 1
        return vec
      }
      idx += 1
    }

    vec
  }

  def updateArea(area: Double, vec: Array[Int]): Array[Int] = {
    if(area < 30) return vec
    var idx = 1
    while(idx < areaRanges.length) {
      if(area >= areaRanges(idx - 1) && area < areaRanges(idx)) {
        vec(idx - 1) += 1
        return vec
      }
      idx += 1
    }

    vec
  }

  def updateRoom(num: Int, vec: Array[Int]): Array[Int] = {
    num match {
      case n if n >= 1 && n <= 4 => vec(n - 1) += 1
      case n if n >= 5 && n <= 10 => vec(5) += 1
    }

    vec
  }

  def updateOrient(map: Map[String, Int], orient: String, vec: mutable.Map[Int, Int]) {
    updateIdFeats(map, orient, vec)
  }

  def updateBuildingAgeRange(age: Int, vec: Array[Int]): Array[Int] = {
    var idx = 1
    while(idx < buildingAgeRanges.length) {
      if(2016 - age >= buildingAgeRanges(idx - 1) && 2016 - age < buildingAgeRanges(idx)) {
        vec(idx - 1) += 1
        return vec
      }
      idx += 1
    }

    vec
  }

  def updateBoolean(value: Int, vec: Array[Int]): Array[Int] = {
    value match {
      case 1 => vec(1) += 1
      case _ => vec(0) += 1
    }

    vec
  }

  def updateUnique(value: String, vec: Array[Int]): Array[Int] = {
    try {
      updateBoolean(value.toInt, vec)
    } catch {
      case _: Exception => vec
    }
  }

  def updateSchoolDistrict(value: Int, vec: Array[Int]): Array[Int] = {
    updateBoolean(value, vec)
  }

  def updateNearMetro(value: String, vec: Array[Int]): Array[Int] = {
    vec
  }

  def updateActiveDateSlot(ts: Long, startTs: Long, endTs: Long, vec: mutable.Map[Int, Int]) {
    if(ts >= startTs && ts < endTs) {
      val idx = ((ts - startTs) / dateSlot).toInt
      if(vec.contains(idx)) {
        vec.put(idx, vec.get(idx).get + 1)
      } else {
        vec.put(idx, 1)
      }
    }
  }

  def updateIps(ip: String, set: mutable.Set[String]) {
    set.add(ip.split("\t").take(3).mkString("."))
  }

  def updateLoc(lon: Float, lat: Float, set: mutable.Set[Loc]) {
    set.add(Loc(lon, lat))
  }

  def getStat(feat1: Map[String, AnyRef], feat2: Map[String, AnyRef], key: String) = {
    val set1 = feat1.get(key).get.asInstanceOf[mutable.Map[Int, Int]].keySet
    val set2 = feat2.get(key).get.asInstanceOf[mutable.Map[Int, Int]].keySet

    val intersect = set1.intersect(set2).size
    val diff = set1.union(set2).size - intersect

    Array(intersect, diff)
  }

  def getIpStat(feat1: Map[String, AnyRef], feat2: Map[String, AnyRef], key: String) = {
    val set1 = feat1.get(key).get.asInstanceOf[mutable.Set[String]].map {ip => ip.split("\\.").take(3).mkString(".")}
    val set2 = feat2.get(key).get.asInstanceOf[mutable.Set[String]].map {ip => ip.split("\\.").take(3).mkString(".")}

    val intersect = set1.intersect(set2).size
    val diff = set1.union(set2).size - intersect

    Array(intersect, diff)
  }

  def getLocStat(feat1: Map[String, AnyRef], feat2: Map[String, AnyRef], key: String) = {
    val set1 = feat1.get(key).get.asInstanceOf[mutable.Set[Loc]]
    val set2 = feat2.get(key).get.asInstanceOf[mutable.Set[Loc]]

    val intersect = set1.intersect(set2).size
    val diff = set1.union(set2).size - intersect

    Array(intersect, diff)
  }

  def genFeatVec(feat1: Map[String, AnyRef], feat2: Map[String, AnyRef], label: Int): (Int, Array[(Int, Int)]) = {

    var idx = 1
    var vec = Seq.empty[(Int, Int)]

    def appendVec(feat: Map[String, AnyRef]) {
      feat.get("price").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("area").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("room").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("bldAge").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("isUnq").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("isSch").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
      feat.get("isMetro").get.asInstanceOf[Array[Int]].foreach { v => vec :+=(idx, v); idx += 1 }
    }

    appendVec(feat1)
    appendVec(feat2)

    getStat(feat1, feat2, "city").foreach { v => vec :+=(idx, v); idx += 1 }
    getStat(feat1, feat2, "district").foreach { v => vec :+=(idx, v); idx += 1 }
    getStat(feat1, feat2, "bizcircle").foreach { v => vec :+=(idx, v); idx += 1 }
    getStat(feat1, feat2, "active").foreach { v => vec :+=(idx, v); idx += 1 }

    getIpStat(feat1, feat2, "ips").foreach { v => vec :+=(idx, v); idx += 1 }
    getLocStat(feat1, feat2, "locations").foreach { v => vec :+=(idx, v); idx += 1 } // todo: 处理

    (label, vec.toArray)
  }

  def genDataset(path: String,
                 startTs: Long,
                 endTs: Long) = {
    /*sc.textFile("/user/bigdata/profiling/online_user_assoc_feat_part_inflate_group_sample")*/

    var dataset = List.empty[(Int, Array[(Int, Int)])]
    var allFeats = List.empty[(String, Map[String, AnyRef])]

    var curUcid = ""
    var curFeats = List.empty[Map[String, AnyRef]]
    Source.fromFile(path).getLines.foreach { line =>

      val Array(ucid, uuid, events) = line.split("\t", 3)

      if(ucid != curUcid) {
        if(ucid != "") {
          for(i <- curFeats.indices) {
            for(j <- i + 1 until curFeats.size) {
              dataset :+= genFeatVec(curFeats(i), curFeats(j), 1)
            }
          }
        }

        curUcid = ucid
        curFeats = List.empty[Map[String, AnyRef]]
        Map.empty[String, AnyRef].hashCode()
      }

      val feat = computeFeature(events, startTs, endTs)

      curFeats :+= feat
      allFeats :+= (ucid, feat)
    }

    if(curUcid != "") {
      for(i <- curFeats.indices) {
        for(j <- i + 1 until curFeats.size) {
          dataset = dataset :+ genFeatVec(curFeats(i), curFeats(j), 1)
        }
      }
    }

    val num = dataset.size
    val len = allFeats.size

    val feats = allFeats.toArray

    var cnt = 0
    while(cnt < num) {
      val i = Random.nextInt(len)
      val j = Random.nextInt(len)

      if(i != j && feats(i)._1 != feats(j)._1) {
        dataset = dataset :+ genFeatVec(feats(i)._2, feats(j)._2, 0)
        cnt += 1
      }
    }

    dataset
  }

  def computeFeature(events: String, startTs: Long, endTs: Long) = {
    val evts = events.split("\t")
    val feats = createFeature()

    for (evt <- evts) {
      try {
        val obj = JSON.parseObject(evt)

        if (obj.containsKey("house")) {
          val house = obj.getJSONObject("house")
          updatePrice(house.getDouble("list_price"), feats.get("price").get.asInstanceOf[Array[Int]])
          updateArea(house.getDouble("area"), feats.get("area").get.asInstanceOf[Array[Int]])
          updateRoom(house.getInteger("bedr_num"), feats.get("room").get.asInstanceOf[Array[Int]])
          updateBuildingAgeRange(house.getInteger("completed_year"), feats.get("bldAge").get.asInstanceOf[Array[Int]])

          updateUnique(house.getString("is_unique"), feats.get("isUnq").get.asInstanceOf[Array[Int]])
          updateSchoolDistrict(house.getInteger("is_school_district"), feats.get("isSch").get.asInstanceOf[Array[Int]])
          updateNearMetro(house.getString("metro_dist_name"), feats.get("isMetro").get.asInstanceOf[Array[Int]])

          updateCity(cities, house.getString("city_id"), feats.get("city").get.asInstanceOf[mutable.Map[Int, Int]])
          updateDistrict(districts, house.getString("district_code"), feats.get("district").get.asInstanceOf[mutable.Map[Int, Int]])
          updateBizCircle(bizCircles, house.getString("bizcircle_code"), feats.get("bizcircle").get.asInstanceOf[mutable.Map[Int, Int]])
          updateOrient(orients, house.getString("orient_id"), feats.get("orient").get.asInstanceOf[mutable.Map[Int, Int]])
        }

        if (obj.containsKey("ts")) updateActiveDateSlot(obj.getLong("ts"), startTs, endTs, feats.get("active").get.asInstanceOf[mutable.Map[Int, Int]])
        if (obj.containsKey("ip")) updateIps(obj.getString("ip"), feats.get("ips").get.asInstanceOf[mutable.Set[String]])
        if (obj.containsKey("lat") && obj.containsKey("lon")) updateLoc(obj.getFloat("lat"), obj.getFloat("lon"),
                                                                        feats.get("loc").get.asInstanceOf[mutable.Set[Loc]])

      } catch {
        case ex: Exception =>
          println(ex.getMessage)
          println(evt)
      }
    }

    feats
  }

  def main(args: Array[String]) {
    val sc = getSc
    val startTs = DateUtil.dateToTimestampMs("20160501")
    val endTs = DateUtil.dateToTimestampMs("20160531")


    // val startTs = DateUtil.dateToTimestampMs(sc.getConf.get("spark.startDate"))
    // val endTs = DateUtil.dateToTimestampMs(sc.getConf.get("spark.endDate"))

    val x = genDataset(dir + "feat_sample", startTs, endTs).map { case (label, feats) =>
      s"$label\t${feats.map { case (index, value) => s"$index:$value" }.mkString("\t")}"
    }

    sc.makeRDD(x).saveAsTextFile("/user/bigdata/profiling/xxsw")
  }

  def getSc = {
    val conf = new SparkConf().setAppName("profiling-assoc-feat")
    if (System.getProperty("os.name") == "Mac OS X") {
      conf.setMaster("local[2]")
      conf.set("spark.startDate", "20160501")
      conf.set("spark.endDate", "20160531")
    }

    new SparkContext(conf)
  }

  def extract(sc: SparkContext, path: String, outputPath: String, startTs: Long, endTs: Long) = {
    sc.textFile(path).map { line =>
      val Array(id, events) = line.split("\t")
      (id, computeFeature(events, startTs, endTs))
    } saveAsObjectFile outputPath
  }
}
