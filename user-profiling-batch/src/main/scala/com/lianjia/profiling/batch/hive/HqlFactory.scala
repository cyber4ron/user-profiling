package com.lianjia.profiling.batch.hive

import com.lianjia.profiling.batch.hive.HqlBuilder.OP

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object HqlFactory {
  def getDeletion(db: String, tbl: String) = {
    HqlBuilder.newHql().dropIfExist(db, tbl).hql()
  }

  def getDelegation(date: String) = {
    HqlBuilder.newHql().select("del.phone_a",         // 0
                               "del.cust_pkid",       // 1
                               "del.name",            // 2
                               "del.state",           // 3
                               "del.created_time",    // 4
                               "del.invalid_time",    // 5
                               "del.holder_time",     // 6
                               "del.created_uid",     // 7
                               "del.created_code",    // 8
                               "del.invalid_uid",     // 9
                               "del.holder_uid",      // 10
                               "del.holder_code",     // 11
                               "del.biz_type",        // 12
                               "del.city_id",         // 13
                               "del.app_id",          // 14
                               "need.district_code",  // 15
                               "need.district_name",  // 16
                               "need.bizcircle_name", // 17
                               "need.room_min",       // 18
                               "need.room_max",       // 19
                               "need.area_min",       // 20
                               "need.area_max",       // 21
                               "need.price_min",      // 22
                               "need.price_max",      // 23
                               "need.balcony",        // 24
                               "need.face")           // 25
    .from("data_center", "dim_merge_custdel_day", "del")
    .join("data_center", "dim_merge_cust_need_day", "need")
    .on("del.cust_pkid", OP.EQ, "need.cust_pkid")
    .and("del.app_id", OP.EQ, "need.app_id")
    .and("need.pt", OP.EQ, s"'${date}000000'")
    .where("del.pt", OP.EQ, s"'${date}000000'")
    .hql()
  }

  def getTouring(date: String) = {
    HqlBuilder.newHql().select("del.phone_a",                 // 0
                               "touring.id",                  // 1
                               "touring.cust_pkid",           // 2
                               "touring.showing_uid",         // 3
                               "touring.showing_broker_code", // 4
                               "touring.showing_begin_time",  // 5
                               "touring.showing_end_time",    // 6
                               "touring.total_feed_back",     // 7
                               "touring.biz_type",            // 8
                               "touring.app_id",              // 9
                               "touring.city_id")             // 10
    .from("data_center", "dim_merge_showing_day", "touring")
    .join("data_center", "dim_merge_custdel_day", "del")
    .on("touring.cust_pkid", OP.EQ, "del.cust_pkid")
    .and("touring.app_id", OP.EQ, "del.app_id")
    .and("del.pt", OP.EQ, s"'${date}000000'")
    .where("touring.pt", OP.EQ, s"'${date}000000'")
    .hql()
  }

  def getTouringHouse(date: String) = {
    HqlBuilder.newHql()
    .select("del.phone_a",                  // 0
            "touring.id as tid",            // 1
            "touring_house.id as thid",     // 2
            "touring_house.created_uid",    // 3
            "touring_house.created_code",   // 4
            "touring_house.created_time",   // 5
            "touring_house.city_id",        // 6
            "house.house_pkid",             // 7
            "house.hdic_house_id",          // 8
            "touring_house.biz_type",       // 9
            "touring_house.frame_type",     // 10
            "touring_house.feedback_type",  // 11
            "house.district_code",          // 12
            "house.district_name",          // 13
            "house.bizcircle_code",         // 14
            "house.bizcircle_name",         // 15
            "house.resblock_id",            // 16
            "house.resblock_name",          // 17
            "house.room_cnt",               // 18
            "round(house.build_area)",      // 19
            "house.signal_floor",           // 20
            "house.floor_name",             // 21
            "house.total_floor",            // 22
            "house.build_end_year",         // 23
            "house.is_sales_tax",           // 24
            "house.distance_metro_code",    // 25
            "house.distance_metro_name",    // 26
            "house.is_school_district",     // 27
            "house.is_unique_house",        // 28
            "house.face",                   // 29
            "round(house.total_prices)")    // 30
    .from("data_center", "dim_merge_showing_house_day", "touring_house")
    .join("data_center", "dim_merge_showing_day", "touring")
    .on("touring_house.showing_id", OP.EQ, "touring.id")
    .and("touring_house.app_id", OP.EQ, "touring.app_id")
    .and("touring.pt", OP.EQ, s"'${date}000000'")
    .join("data_center", "dim_merge_custdel_day", "del")
    .on("touring.cust_pkid", OP.EQ, "del.cust_pkid")
    .and("touring.app_id", OP.EQ, "del.app_id")
    .and("del.pt", OP.EQ, s"'${date}000000'")
    .join("data_center", "dim_merge_house_day", "house")
    .on("touring_house.house_pkid", OP.EQ, "house.house_pkid")
    .and("touring_house.app_id", OP.EQ, "house.app_id")
    .and("house.pt", OP.EQ, s"'${date}000000'")
    .where("touring_house.pt", OP.EQ, s"'${date}000000'")
    .hql()
  }

  def getContractOld(date: String) = {
    HqlBuilder.newHql()
    .select("del.phone_a",                // 0
            "contr.cott_pkid",            // 1
            "contr.cust_pkid",            // 2
            "contr.house_pkid",           // 3
            "contr.hdic_house_id",        // 4
            "contr.state",                // 5
            "contr.deal_time",            // 6
            "round(contr.realmoney)",     // 7
            "contr.created_uid",          // 8
            "contr.created_code",         // 9
            "contr.created_time",         // 10
            "contr.biz_type",             // 11
            "contr.city_id",              // 12
            "contr.app_id",               // 13
            "house.district_code",        // 14
            "house.district_name",        // 15
            "house.bizcircle_code",       // 16
            "house.bizcircle_name",       // 17
            "house.resblock_id",          // 18
            "house.resblock_name",        // 19
            "house.room_cnt",             // 20
            "round(house.build_area)",    // 21
            "house.signal_floor",         // 22
            "house.floor_name",           // 23
            "house.total_floor",          // 24
            "house.build_end_year",       // 25
            "house.is_sales_tax",         // 26
            "house.distance_metro_code",  // 27
            "house.distance_metro_name",  // 28
            "house.is_school_district",   // 29
            "house.is_unique_house",      // 30
            "house.face",                 // 31
            "round(house.total_prices)")  // 32
    .from("data_center", "dim_merge_contract_day", "contr")
    .join("data_center", "dim_merge_custdel_day", "del")
    .on("contr.cust_pkid", OP.EQ, "del.cust_pkid")
    .and("contr.app_id", OP.EQ, "del.app_id")
    .and("contr.pt", OP.EQ, s"'${date}000000'")
    .and("del.pt", OP.EQ, s"'${date}000000'")
    .join("data_center", "dim_merge_house_day", "house")
    .on("contr.house_pkid", OP.EQ, "house.house_pkid")
    .and("contr.app_id", OP.EQ, "house.app_id")
    .and("house.pt", OP.EQ, s"'${date}000000'")
    .hql()
  }

  def getContract(date: String) = {
    s"""select del.phone_a,           -- 0
       |contr.cott_pkid,              -- 1
       |contr.cust_pkid,              -- 2
       |contr.house_pkid,             -- 3
       |contr.hdic_house_id,          -- 4
       |contr.state,                  -- 5
       |contr.deal_time,              -- 6
       |round(contr.realmoney),       -- 7
       |contr.created_uid,            -- 8
       |contr.created_code,           -- 9
       |contr.created_time,           -- 10
       |contr.biz_type,               -- 11
       |contr.city_id,                -- 12
       |contr.app_id,                 -- 13
       |house.district_code,          -- 14
       |house.district_name,          -- 15
       |house.bizcircle_code,         -- 16
       |house.bizcircle_name,         -- 17
       |house.resblock_id,            -- 18
       |house.resblock_name,          -- 19
       |house.room_cnt,               -- 20
       |round(house.build_area),      -- 21
       |house.signal_floor,           -- 22
       |house.floor_name,             -- 23
       |house.total_floor,            -- 24
       |house.build_end_year,         -- 25
       |house.is_sales_tax,           -- 26
       |house.distance_metro_code,    -- 27
       |house.distance_metro_name,    -- 28
       |house.is_school_district,     -- 29
       |house.is_unique_house,        -- 30
       |house.face,                   -- 31
       |round(house.total_prices)     -- 32
       |from (select cott_pkid,
       |             case length(cust_pkid)
       |                 when 10 then concat('5010', substr(cust_pkid, -8))
       |                 when 11 then concat('501', substr(cust_pkid, -9))
       |                 when 12 then concat('50', substr(cust_pkid, -10))
       |                 else null
       |             end as cust_pkid,
       |             house_pkid,
       |             hdic_house_id,
       |             state,
       |             deal_time,
       |             realmoney,
       |             created_uid,
       |             created_code,
       |             created_time,
       |             biz_type,
       |             city_id,
       |             app_id
       |      from data_center.dim_merge_contract_day ct
       |      where ct.pt = '${date}000000') contr
       |join data_center.dim_merge_custdel_day as del
       |on contr.cust_pkid = del.cust_pkid
       |-- and contr.app_id = del.app_id
       |and del.pt = '${date}000000'
       |join data_center.dim_merge_house_day as house
       |on substr(contr.house_pkid, -8) = substr(house.house_pkid, -8) and contr.hdic_house_id = house.hdic_house_id
       |-- and contr.app_id = house.app_id
       |and house.pt = '${date}000000'
     """.stripMargin
  }

  def getHouse(date: String) = {
    HqlBuilder.newHql()
    .select("house_pkid",          // 0
            "hdic_house_id",       // 1
            "created_uid",         // 2
            "created_code",        // 3
            "state",               // 4
            "created_time",        // 5
            "invalid_time",        // 6
            "holder_time",         // 7
            "biz_type",            // 8
            "round(total_prices)", // 9
            "face",                // 10
            "room_cnt",            // 11
            "parlor_cnt",          // 12
            "cookroom_cnt",        // 13
            "toilet_cnt",          // 14
            "balcony_cnt",         // 15
            "round(build_area)",   // 16
            "district_code",       // 17
            "district_name",       // 18
            "bizcircle_code",      // 19
            "bizcircle_name",      // 20
            "resblock_id",         // 21
            "resblock_name",       // 22
            "building_id",         // 23
            "building_name",       // 24
            "unit_code",           // 25
            "unit_name",           // 26
            "signal_floor",        // 27
            "floor_name",          // 28
            "house_usage_code",    // 29
            "house_usage_name",    // 30
            "total_floor",         // 31
            "build_end_year",      // 32
            "is_sales_tax",        // 33
            "distance_metro_code", // 34
            "distance_metro_name", // 35
            "is_school_district",  // 36
            "fitment_type_code",   // 37
            "fitment_type_name",   // 38
            "city_id",             // 39
            "city_name",           // 40
            "is_unique_house",     // 41
            "app_id")              // 42
    .from("data_center", "dim_merge_house_day", "house")
    .where("house.pt", OP.EQ, s"'${date}000000'")
    .hql()
  }
}
