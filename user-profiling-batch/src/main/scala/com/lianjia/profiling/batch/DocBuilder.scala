package com.lianjia.profiling.batch

import com.lianjia.profiling.common.MapBuilder
import com.lianjia.profiling.common.elasticsearch.Types.{Document, JDocument}
import com.lianjia.profiling.util.FieldUtil

/**
  * @author fenglei@lianjia.com on 2016-03.
  */
object DocBuilder {
  private val DEL_FIELD_NUM = 26
  private val TOURING_FIELD_NUM = 11
  private val TOURING_HOUSE_FIELD_NUM = 31
  private val CONTRACT_FIELD_NUM = 33
  private val HOUSE_FIELD_NUM = 43

  case class Doc(id: String, doc: Document)
  case class JDoc(id: String, doc: JDocument)
  case class NestedDoc(rootId: String, id: String, doc: Document)

  case class NestedLv2Doc(rootId: String, lv1Id: String, id: String, doc: Document)
  case class ChildDoc(parent: String, id: String, doc: Document)

  def buildCust(line: String): Option[Doc] = {
    val fields = line.split('\t')
    if (fields.length != DEL_FIELD_NUM || !FieldUtil.isFieldValid(fields(0))) {
      System.err.println(s"fields.length != DEL_FIELD_NUM || !FieldUtil.isFieldValid(fields(0)), line: $line")
      return None
    }
    if (FieldUtil.parsePhone(fields(0)) == null) {
      System.err.println(s"parse phone failed, line: $line")
      return None
    }
    Some(Doc(FieldUtil.parsePhone(fields(0)), MapBuilder.newMap
                        .addText("phone", FieldUtil.parsePhone(fields(0)))
                        .addText("name", fields(2))
                        .addDateTime("write_ts", System.currentTimeMillis)
                        .getDoc))
  }

  def buildDelDoc(fields: Array[String]) = { // todo mapping field name可以缩写; text -> long
    MapBuilder.newMap
    .addText("del_id",            fields(1))
    .addLong("state",             fields(3))
    .addDateTime("creation_time", fields(4))
    .addDateTime("invalid_time",  fields(5))
    .addDateTime("update_time",   fields(6))
    .addLong("creator_uid",       fields(7))
    .addLong("creator_code",      fields(8))
    .addLong("invalidator_uid",   fields(9))
    .addLong("updater_uid",       fields(10))
    .addLong("updater_code",      fields(11))
    .addLong("biz_type",          fields(12))
    .addLong("city_id",           fields(13))
    .addLong("app_id",            fields(14))
    .addLong("district_code",     fields(15))
    .addText("district",          fields(16))
    .addText("biz_circle",        fields(17))
    .addInt("room_min",           fields(18))
    .addInt("room_max",           fields(19))
    .addDouble("area_min",        fields(20))
    .addDouble("area_max",        fields(21))
    .addDouble("price_min",       fields(22))
    .addDouble("price_max",       fields(23))
    .addLong("balcony_need",      fields(24))
    .addLong("orientation",       fields(25))
    .addDateTime("write_ts", System.currentTimeMillis)
  }

  def buildDel(line: String): Option[NestedDoc] = {
    val fields = line.split('\t')

    if (fields.length != DEL_FIELD_NUM) {
      System.err.println(s"fields.length != DEL_FIELD_NUM, line: $line")
      return None
    }

    if (FieldUtil.parsePhone(fields(0)) == null) {
      System.err.println(s"parse phone failed, line: $line")
      return None
    }

    Some(NestedDoc(FieldUtil.parsePhone(fields(0)), fields(1), buildDelDoc(fields).getDoc))
  }

  def buildDelFlatten(line: String): Option[Doc] = {
    val fields = line.split('\t')

    if (fields.length != DEL_FIELD_NUM || FieldUtil.parsePhone(fields(0)) == null) return None

    Some(Doc(fields(1), buildDelDoc(fields)
                        .addText("phone", FieldUtil.parsePhone(fields(0)))
                        .addText("name", fields(2))
                        .getDoc))
  }

  def BuildTouring(line: String): Option[NestedDoc] = {
    val fields = line.split('\t')

    if (fields.length != TOURING_FIELD_NUM) return None
    if (FieldUtil.parsePhone(fields(0)) == null) return None

    Some(NestedDoc(FieldUtil.parsePhone(fields(0)), fields(1), MapBuilder.newMap
                                         .addLong("touring_id",       fields(1))
                                         .addText("del_id",           fields(2))
                                         .addLong("broker_ucid",      fields(3))
                                         .addText("broker_code",      fields(4))
                                         .addDateTime("begin_time",   fields(5))
                                         .addDateTime("end_time",     fields(6))
                                         .addText("feedback",         fields(7)) // todo 中文分词ik
                                         .addLong("biz_type",         fields(8))
                                         .addLong("app_id",           fields(9))
                                         .addLong("city_id",          fields(10))
                                         .addDateTime("write_ts", System.currentTimeMillis)
                                         .getDoc))
  }

  /**
    * @see  http://data.lianjia.com:8081/web/index.php?r=dict/detail&table_name=dim_merge_house_day
    *       http://data.lianjia.com:8081/web/index.php?r=dict/detail&table_name=dim_merge_showing_house_day
    */
  def buildTouringHouseDoc(fields: Array[String]) = {
    MapBuilder.newMap
    .addLong("touring_house_id",   fields(2))
    .addText("creation_ucid",      fields(3))
    .addText("creation_code",      fields(4))
    .addDateTime("creation_date",  fields(5))
    .addLong("city_id",            fields(6))
    .addText("house_id",           fields(7))
    .addLong("hdic_house_id",      fields(8))
    .addLong("biz_type",           fields(9))
    .addLong("structure_type",     fields(10))
    .addLong("feedback_type",      fields(11))
    .addText("district_code",      fields(12))
    .addText("district_name",      fields(13))
    .addText("bizcircle_id",       fields(14))
    .addText("bizcircle_name",     fields(15))
    .addText("resblock_id",        fields(16))
    .addText("resblock_name",      fields(17))
    .addInt("room_cnt",            fields(18))
    .addDouble("area",             fields(19))
    .addText("floor_no",           fields(20))
    .addText("floor_name",         fields(21))
    .addText("floors_num",         fields(22))
    .addInt("completed_year",      fields(23))
    .addInt("has_sale_tax",       fields(24))
    .addText("metro_dist_code",    fields(25))
    .addText("metro_dist_name",    fields(26))
    .addInt("is_school_district", fields(27))
    .addInt("is_unique",           fields(28))
    .addLong("orient_id",          fields(29))
    .addDouble("list_price",       fields(30))
    .addDateTime("write_ts", System.currentTimeMillis)
  }

  def buildTouringHouse(line: String): Option[NestedLv2Doc] = {
    val fields = line.split('\t')

    if (fields.length != TOURING_HOUSE_FIELD_NUM) return None
    if (FieldUtil.parsePhone(fields(0)) == null) return None

    val doc = buildTouringHouseDoc(fields).getDoc

    // patch 20160707, 带看房屋表的id有时为null
    val touringHouseId = Math.abs((doc.getOrElse("hdic_house_id", "null").toString +
      doc.getOrElse("creation_date", "null").toString +
      doc.getOrElse("creation_code", "null").toString +
      FieldUtil.parsePhone(fields(0))).hashCode).toString

    Some(NestedLv2Doc(FieldUtil.parsePhone(fields(0)), fields(1), touringHouseId, doc))
  }

  def buildTouringHouseFlatten(line: String): Option[Doc] = {
    val fields = line.split('\t')

    if (fields.length != TOURING_HOUSE_FIELD_NUM) return None

    val doc = buildTouringHouseDoc(fields)
              .addText("phone", FieldUtil.parsePhone(fields(0)))
              .addText("touring_id", fields(1))
              .getDoc

    // patch 20160707, 带看房屋表的id有时为null
    val touringHouseId = Math.abs((doc.getOrElse("hdic_house_id", "null").toString +
      doc.getOrElse("creation_date", "null").toString +
      doc.getOrElse("creation_code", "null").toString +
      FieldUtil.parsePhone(fields(0))).hashCode).toString

    Some(Doc(touringHouseId, doc))
  }

  def BuildContractDoc(fields: Array[String]) = {
    MapBuilder.newMap
    .addText("contract_id",        fields(1))
    .addText("cust_pkid",          fields(2))
    .addText("house_pkid",         fields(3))
    .addLong("hdic_house_id",      fields(4))
    .addLong("state",              fields(5))
    .addDateTime("deal_time",      fields(6))
    .addDouble("price",            fields(7))
    .addLong("creator_uid",        fields(8))
    .addLong("creator_code",       fields(9))
    .addDateTime("created_time",   fields(10))
    .addLong("biz_type",           fields(11))
    .addLong("city_id",            fields(12))
    .addLong("app_id",             fields(13))
    .addText("district_code",      fields(14))
    .addText("district_name",      fields(15))
    .addText("bizcircle_id",       fields(16))
    .addText("bizcircle_name",     fields(17))
    .addText("resblock_id",        fields(18))
    .addText("resblock_name",      fields(19))
    .addInt("room_cnt",            fields(20))
    .addDouble("area",             fields(21))
    .addText("floor_no",           fields(22))
    .addText("floor_name",         fields(23))
    .addText("floors_num",         fields(24))
    .addInt("completed_year",      fields(25))
    .addInt("has_sale_tax",        fields(26))
    .addText("metro_dist_code",    fields(27))
    .addText("metro_dist_name",    fields(28))
    .addInt("is_school_district",  fields(29))
    .addInt("is_unique",           fields(30))
    .addLong("orient_id",          fields(31))
    .addDouble("list_price",       fields(32))
    .addDateTime("write_ts", System.currentTimeMillis)
  }

  def BuildContract(line: String): Option[NestedDoc] = {
    val fields = line.split('\t')

    if (fields.length != CONTRACT_FIELD_NUM) return None
    if (FieldUtil.parsePhone(fields(0)) == null) return None

    Some(NestedDoc(FieldUtil.parsePhone(fields(0)), fields(1), BuildContractDoc(fields).getDoc))
  }

  def BuildContractFlatten(line: String): Option[Doc] = {
    val fields = line.split('\t')

    if (fields.length != CONTRACT_FIELD_NUM) return None
    if (FieldUtil.parsePhone(fields(0)) == null) return None

    Some(Doc(fields(1), BuildContractDoc(fields)
                        .addText("phone", FieldUtil.parsePhone(fields(0)))
                        .getDoc))
  }

  def buildHouse(line: String): Option[Doc] = {
    val fields = line.split('\t')

    if (fields.length != HOUSE_FIELD_NUM) return None

    Some(Doc(fields(0), MapBuilder.newMap
                        .addText("house_id",                        fields(0))
                        .addLong("hdic_id",                         fields(1))
                        .addLong("creator_ucid",                    fields(2))
                        .addLong("creator_id",                      fields(3))
                        .addText("status",                          fields(4))
                        .addDateTime("creation_date",               fields(5))
                        .addDateTime("invalid_date",                fields(6))
                        .addDateTime("update_date",                 fields(7))
                        .addLong("biz_type",                        fields(8))
                        .addDouble("list_price",                    fields(9))
                        .addLong("orient_id",                       fields(10))
                        .addInt("bedr_num",                         fields(11))
                        .addInt("livingr_num",                      fields(12))
                        .addInt("kitchen_num",                      fields(13))
                        .addInt("bathr_num",                        fields(14))
                        .addInt("balcony_num",                      fields(15))
                        .addDouble("area",                          fields(16))
                        .addText("district_code",                   fields(17))
                        .addText("district_name",                   fields(18))
                        .addText("bizcircle_code",                  fields(19))
                        .addText("bizcircle_name",                  fields(20))
                        .addText("resblock_id",                     fields(21))
                        .addText("resblock_name",                   fields(22))
                        .addText("building_id",                     fields(23))
                        .addText("building_name",                   fields(24))
                        .addText("unit_code",                       fields(25))
                        .addText("unit_name",                       fields(26))
                        .addText("floor_no",                        fields(27))
                        .addText("floor_name",                      fields(28))
                        .addText("house_usage_code",                fields(29))
                        .addText("house_usage_name",                fields(30))
                        .addInt("floors_num",                       fields(31))
                        .addInt("completed_year",                   fields(32))
                        .addIntIgnoreInvalid("has_sale_tax",        fields(33), 0)
                        .addText("metro_dist_code",                 fields(34))
                        .addText("metro_dist_name",                 fields(35))
                        .addIntIgnoreInvalid("is_school_district",  fields(36), 0)
                        .addText("fitment_type_code",               fields(37))
                        .addText("fitment_type_name",               fields(38))
                        .addLong("city_id",                         fields(39))
                        .addText("city_name",                       fields(40))
                        .addIntIgnoreInvalid("is_unique",           fields(41), -1)
                        .addLong("app_id",                          fields(42))
                        .addDateTime("write_ts", System.currentTimeMillis)
                        .getDoc))
  }
}
