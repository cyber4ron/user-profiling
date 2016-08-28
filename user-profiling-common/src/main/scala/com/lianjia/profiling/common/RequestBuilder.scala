package com.lianjia.profiling.common

import com.lianjia.profiling.common.elasticsearch.Types
import com.lianjia.profiling.common.elasticsearch.Types._
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.script.{Script, ScriptService}

import scala.collection.JavaConverters._

/**
  * @author fenglei@lianjia.com on 2016-03.
  */

object RequestBuilder {
  def toPlainJava(any: Any): AnyRef = any match {
    case x: Seq[Map[String, AnyRef]] => x.map(item => toPlainJava(item)).asJava
    case x: Map[String, AnyRef] => x.map(item => (item._1, toPlainJava(item._2))).asJava
    case x => x.asInstanceOf[AnyRef]
  }

  def newReq(index: String, idxType: String, id: String) = new RequestBuilder(index, idxType, id)

  def newReq() = new RequestBuilder()

  case class ScriptedUpdate(script: String, params: Params, upsertDoc: Doc = Map())
}

class RequestBuilder private(var index: String = "", var idxType: String = "", var id: String = "") {

  import Types._
  import RequestBuilder._

  private var requests = Seq.empty[UpdateRequest]

  def setIdentity(_index: String, _idxType: String, _id: String) = {
    index = _index
    idxType = _idxType
    id = _id
    this
  }

  def addUpsertReq(doc: Doc, parent: String) = {
    if (index.equals("") || idxType.equals("") || id.equals("")) {
      throw new IllegalStateException("index, idxType and id can not be empty.")
    }
    requests = requests :+ new UpdateRequest(index, idxType, id)
                           .docAsUpsert(true)
                           .parent(parent)
                           // cast为AnyRef的map在匹配函数时会被作为Object(Java). 另外也可以转换为json字符串
                           .doc(doc.map(kv => kv._1 -> toPlainJava(kv._2)).asJava)
    this
  }

  def addUpsertReq(doc: JDocument, parent: String) = {
    if (index.equals("") || idxType.equals("") || id.equals("")) {
      throw new IllegalStateException("index, idxType and id can not be empty.")
    }
    requests = requests :+ new UpdateRequest(index, idxType, id)
                           .docAsUpsert(true)
                           .parent(parent)
                           .doc(doc)
    this
  }

  def addUpsertReq(doc: JDocument) = {
    if (index.equals("") || idxType.equals("") || id.equals("")) {
      throw new IllegalStateException("index, idxType and id can not be empty.")
    }
    requests = requests :+ new UpdateRequest(index, idxType, id)
                           .docAsUpsert(true)
                           .doc(doc)
    this
  }

  def addUpsertReqs(doc: Seq[Doc]) = {
    doc.foreach(addUpsertReq)
    this
  }

  def addUpsertReq(doc: Doc) = {
    if (index.equals("") || idxType.equals("") || id.equals("")) {
      throw new IllegalStateException("index, idxType and id must not empty.")
    }
    requests = requests :+ new UpdateRequest(index, idxType, id)
                           .docAsUpsert(true)
                           .doc(doc.map(kv => kv._1 -> toPlainJava(kv._2)).asJava)
    this
  }

  def addFieldScriptedUpdates(updates: Seq[ScriptedUpdate]) = {
    updates.foreach(update => addFieldScriptedUpdate(update))
    this
  }

  def addFieldScriptedUpdate(update: ScriptedUpdate) = {
    val ScriptedUpdate(script, params, upsertDoc) = update
    val paramsJava = toPlainJava(params).asInstanceOf[JDocument]
    requests = requests :+ new UpdateRequest(index, idxType, id)
                           .upsert(toPlainJava(upsertDoc).asInstanceOf[JDocument])
                           .script(new Script(script,
                                              ScriptService.ScriptType.INLINE,
                                              null,
                                              paramsJava))
    this
  }

  def get() = requests
}
