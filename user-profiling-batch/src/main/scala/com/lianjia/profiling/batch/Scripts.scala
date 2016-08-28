package com.lianjia.profiling.batch

/**
  * @author fenglei@lianjia.com on 2016-04
  */

object Scripts {
  private val upsertTmplLv1 = // upsert doc to nested field
    """if (ctx._source.%field == null) {  // 以tourings为例, tourings不存在
      |  ctx._source.%root_id = rootId;
      |  ctx._source.%field = [doc];
      |} else { // tourings存在, upsert touring信息
      |  it = ctx._source.%field.iterator();
      |  found = false
      |  while(it.hasNext()) { // update相同touring的信息
      |    item = it.next()
      |    if (item.%id && item.%id.toString().equals(id.toString())) {
      |    found = true
      |      for (e in doc) { // insert touring信息
      |        item.put(e.key, e.value)
      |      }
      |    }
      |  }
      |  if(!found) ctx._source.%field.add(doc); //  append touring doc
      |}
      | """.stripMargin

  private val upsertTmplLv2 = // upsert doc to 2-level nested field
    """if (ctx._source.%lv1_field == null) { // 以touring_house为例. tourings不存在
      |  ctx._source.%root_id = rootId
      |  ctx._source.%lv1_field = [["%lv1_id": lv1Id, // touring_id->xxx
      |                             "%lv2_field": [doc]]]; // touring_house_id -> xxx
      |} else { // tourings存在, 遍历tourings
      |  it = ctx._source.%lv1_field.iterator();
      |  find = false;
      |  for (e in ctx._source.%lv1_field) if (e.%lv1_id && e.%lv1_id.toString().equals(lv1Id.toString())) { // 删掉touring_house_id相同的并append
      |    find = true;
      |    if(e.%lv2_field != null) { // houses存在
      |      it = e.%lv2_field.iterator();
      |      while(it.hasNext()) { // 删掉touring_house_id相同的
      |        x = it.next()
      |        if (x.%id != null && x.%id.toString().equals(id.toString())) it.remove();
      |      }
      |    } else { // houses不存在
      |      e.%lv2_field = []
      |    }
      |    e.%lv2_field.add(doc); // append house doc
      |    break;
      |  }
      |  if(!find) { // 不存在对应touring_id, 添加
      |    ctx._source.%lv1_field.add(["%lv1_id": lv1Id,
      |                                "%lv2_field": [doc]])
      |  }
      |}
      | """.stripMargin


  private val scripts = Map("customer/customer/delegations" -> upsertTmplLv1.replace("%root_id", "phone")
                                                               .replace("%id", "del_id")
                                                               .replace("%field", "delegations"),
                            "customer/customer/tourings" -> upsertTmplLv1.replace("%root_id", "phone")
                                                            .replace("%id", "touring_id")
                                                            .replace("%field", "tourings"),
                            "customer/customer/tourings.houses" -> upsertTmplLv2.replace("%root_id", "phone")
                                                                   .replace("%lv1_id", "touring_id")
                                                                   .replace("%id", "touring_house_id")
                                                                   .replace("%lv1_field", "tourings")
                                                                   .replace("%lv2_field", "houses"),
                            "customer/customer/contracts" -> upsertTmplLv1.replace("%root_id", "phone")
                                                             .replace("%id", "contract_id")
                                                             .replace("%field", "contracts"))

  def getScript(key: String) = {
    scripts.getOrElse(key, null)
  }
}
