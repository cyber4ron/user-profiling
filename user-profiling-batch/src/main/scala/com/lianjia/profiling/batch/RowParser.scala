package com.lianjia.profiling.batch

import com.lianjia.profiling.common.RequestBuilder
import com.lianjia.profiling.common.RequestBuilder.ScriptedUpdate
import com.lianjia.profiling.tagging.house.HouseTagging
import com.lianjia.profiling.util.Properties
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object RowParser {
  val customerIdx = Properties.getOrDefault("spark.es.customer.idx", "customer")
  val customerIdxType = Properties.getOrDefault("spark.es.customer.idx.type", "customer")

  val delIdx = Properties.getOrDefault("spark.es.delegation.idx", "customer_delegation")
  val delIdxType = Properties.getOrDefault("spark.es.delegation.idx.type", "delegation")

  val touringIdx = Properties.getOrDefault("spark.es.touring.idx", "customer_touring")
  val touringIdxType = Properties.getOrDefault("spark.es.touring.idx.type", "touring")

  val contractIdx = Properties.getOrDefault("spark.es.contract.idx", "customer_contract")
  val contractIdxType = Properties.getOrDefault("spark.es.contract.idx.type", "contract")

  val houseIdx = Properties.getOrDefault("spark.es.house.idx", "house")
  val houseIdxType = Properties.getOrDefault("spark.es.house.idx.type", "house")

  val housePropIdx = Properties.getOrDefault("spark.es.house.prop.idx", "house_prop")
  val housePropIdxType = Properties.getOrDefault("spark.es.house.prop.idx.type", "prop")

  def parseDel(row: String): Seq[UpdateRequest] = {
    val reqs = RequestBuilder.newReq()
    DocBuilder.buildCust(row).map(x => reqs.setIdentity(customerIdx, customerIdxType, x.id).addUpsertReq(x.doc))
    DocBuilder.buildDel(row).map(x => reqs.setIdentity(customerIdx, customerIdxType, x.rootId)
                                      .addFieldScriptedUpdate(ScriptedUpdate(Scripts.getScript("customer/customer/delegations"),
                                                                             Map("rootId" -> x.rootId,
                                                                                 "id" -> x.id,
                                                                                 "doc" -> x.doc),
                                                                             Map("phone" -> x.rootId,
                                                                                 "delegations" -> Seq(x.doc)))))
    DocBuilder.buildDelFlatten(row).map(x => reqs.setIdentity(delIdx, delIdxType, x.id).addUpsertReq(x.doc))

    reqs.get()
  }

  def parseTouring(row: String): Seq[UpdateRequest] = {
    val reqs = RequestBuilder.newReq()
    DocBuilder.BuildTouring(row).map(x => reqs.setIdentity(customerIdx, customerIdxType, x.rootId)
                                          .addFieldScriptedUpdate(ScriptedUpdate(Scripts.getScript("customer/customer/tourings"),
                                                                                 Map("rootId" -> x.rootId,
                                                                                     "id" -> x.id,
                                                                                     "doc" -> x.doc),
                                                                                 Map("phone" -> x.rootId,
                                                                                     "tourings" -> Seq(x.doc)))))
    reqs.get()
  }

  def parseTouringHouse(row: String): Seq[UpdateRequest] = {
    val reqs = RequestBuilder.newReq()
    DocBuilder.buildTouringHouse(row).map(x => reqs.setIdentity(customerIdx, customerIdxType, x.rootId)
                                               .addFieldScriptedUpdate(ScriptedUpdate(Scripts.getScript("customer/customer/tourings.houses"),
                                                                                      Map("rootId" -> x.rootId,
                                                                                          "lv1Id" -> x.lv1Id,
                                                                                          "id" -> x.id,
                                                                                          "doc" -> x.doc),
                                                                                      Map("phone" -> x.rootId,
                                                                                          "tourings" -> Seq(Map("touring_id" -> x.lv1Id,
                                                                                                                "houses" -> Seq(x.doc)))))))
    DocBuilder.buildTouringHouseFlatten(row).map(x => reqs.setIdentity(touringIdx, touringIdxType, x.id).addUpsertReq(x.doc))

    reqs.get()
  }

  def parseContract(row: String): Seq[UpdateRequest] = {
    val reqs = RequestBuilder.newReq()
    DocBuilder.BuildContract(row).map(x => reqs.setIdentity(customerIdx, customerIdxType, x.rootId)
                                           .addFieldScriptedUpdate(ScriptedUpdate(Scripts.getScript("customer/customer/contracts"),
                                                                                  Map("rootId" -> x.rootId,
                                                                                      "id" -> x.id,
                                                                                      "doc" -> x.doc),
                                                                                  Map("phone" -> x.rootId,
                                                                                      "contracts" -> Seq(x.doc)))))
    DocBuilder.BuildContractFlatten(row).map(x => reqs.setIdentity(contractIdx, contractIdxType, x.id).addUpsertReq(x.doc))

    reqs.get()
  }

  def parseHouse(row: String): Seq[UpdateRequest] = {
    val reqs = RequestBuilder.newReq()
    DocBuilder.buildHouse(row).map(x => {
      reqs.setIdentity(houseIdx, houseIdxType, x.id).addUpsertReq(x.doc)
      val jDoc = x.doc.asJava
      val prop = HouseTagging.compute(jDoc)
      reqs.setIdentity(housePropIdx, housePropIdxType, prop.getId).addUpsertReq(prop.toMap)
    })

    reqs.get()
  }
}
