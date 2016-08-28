package com.lianjia.profiling.stream

import com.lianjia.profiling.common.{Logging, RequestBuilder}
import com.lianjia.profiling.common.RequestBuilder.ScriptedUpdate
import com.lianjia.profiling.config.Constants
import com.lianjia.profiling.util.{ExUtil, FieldUtil}
import org.apache.commons.lang3.StringEscapeUtils._
import org.elasticsearch.action.update.UpdateRequest
import spray.json._

import scala.collection.mutable

/**
  * @author fenglei@lianjia.com on 2016-03.
  */
object MessageParserScala extends Logging {
  private val logger = getLogger(MessageParserScala.getClass.getName)

  def parseKafkaMessage(line: String): Seq[UpdateRequest] = {
    try {
      val (msgType, msg) = line.extract()
      val bizJs = msg.unescape().removeQuotes()
      val js = bizJs.parseJson.asJsObject

      val id = getStringField(js, "user_id").getOrElse(getStringField(js, "uuid").getOrElse("null"))
      if (!FieldUtil.isFieldValid(id)) return Seq.empty[UpdateRequest] //  看看数量, log下来?

      val reqs = RequestBuilder.newReq(Constants.ONLINE_USER_IDX, "user", id)
      getStringField(js, "uuid") foreach { x =>
        reqs.addUpsertReqs(Array(Map("uuid" -> x)))
      }

      getStringField(js, "user_id") foreach { x =>
        reqs.addUpsertReqs(Array(Map("user_id" -> x)))
      }

      var evt = mutable.Map.empty[String, AnyRef]
      getStringField(js, "timestamp").foreach(x => evt += "timestamp" -> x.toString)
      for (lon <- getStringField(js, "longitude"); lat <- getStringField(js, "latitude")) {
        evt += "location" -> s"${lat.toString},${lon.toString}"
      }

      msgType match {
        case "[bigdata.mobile]" => parseMobile(js, evt, reqs)

        case "[bigdata.pagevisit]" =>
          // var evt = Map.empty[String, AnyRef]
          getStringField(js, "session_id").foreach(x => evt += "session_id" -> x)
          getStringField(js, "current_page").foreach(x => evt += "current_page" -> x)
          getStringField(js, "referer_page").foreach(x => evt += "referer_page" -> x)
          getStringField(js, "city_id").foreach(x => evt += "city_id" -> x.toString)
          getStringField(js, "channel_id").foreach(x => evt += "channel_id" -> x)
          getStringField(js, "is_search").foreach(x => evt += "is_search" -> x)
          getStringField(js, "keywords").foreach(x => evt += "keywords" -> x)
          reqs.addFieldScriptedUpdate(ScriptedUpdate("page_visits", Map("doc" -> evt)))

        case "[bigdata.detail]" =>
          getStringField(js, "session_id").foreach(x => evt += "session_id" -> x)
          getStringField(js, "current_page").foreach(x => evt += "current_page" -> x)
          getStringField(js, "referer_page").foreach(x => evt += "referer_page" -> x)
          getStringField(js, "city_id").foreach(x => evt += "city_id" -> x)
          getStringField(js, "channel_id").foreach(x => evt += "channel_id" -> x)
          getStringField(js, "is_search").foreach(x => evt += "is_search" -> x)
          getStringField(js, "keywords").foreach(x => evt += "keywords" -> x)
          getStringField(js, "detail_id").foreach(x => evt += "detail_id" -> x)
          getStringField(js, "detail_type").foreach(x => evt += "detail_type" -> x)
          reqs.addFieldScriptedUpdate(ScriptedUpdate("page_visit_details", Map("doc" -> evt)))

        case "[bigdata.search]" =>
          getStringField(js, "session_id").foreach(x => evt += "session_id" -> x)
          getStringField(js, "city_id").foreach(x => evt += "city_id" -> x)
          getStringField(js, "channel_id").foreach(x => evt += "channel_id" -> x)
          getStringField(js, "out_keywords").foreach(x => evt += "out_keywords" -> x)
          getStringField(js, "keywords").foreach(x => evt += "keywords" -> x)
          reqs.addFieldScriptedUpdate(ScriptedUpdate("searches", Map("doc" -> evt)))

        case "[bigdata.follow]" =>
          getStringField(js, "session_id").foreach(x => evt += "session_id" -> x)
          getStringField(js, "attention_id").foreach(x => evt += "follow_id" -> x)
          getStringField(js, "attention_type").foreach(x => evt += "follow_type" -> x)
          reqs.addFieldScriptedUpdate(ScriptedUpdate("follows", Map("doc" -> evt)))

        case "[bigdata.calculator]" =>
          getStringField(js, "session_id").foreach(x => evt += "session_id" -> x)
          getStringField(js, "loan_type").foreach(x => evt += "loan_type" -> x)
          getStringField(js, "commercial_loan_type").foreach(x => evt += "commercial_loan_type" -> x)
          getStringField(js, "mortgage_month_amount").foreach(x => evt += "mortgage_month_amount" -> x)
          reqs.addFieldScriptedUpdate(ScriptedUpdate("calculators", Map("doc" -> evt)))

        case _ =>
          logger.warn(s"[bigdata.*] match failed, message: $line")
      }

      reqs.get()
    } catch {
      case ex: Exception =>
        logger.warn(s"parse kafka message failed, message: $line, ex.stacktrace: ${ExUtil.getStackTrace(ex)}")
        Seq.empty[UpdateRequest]
    }
  }

  def parseMobile(js: JsObject, evt: mutable.Map[String, AnyRef], reqs: RequestBuilder) {
    js.fields.get("events").foreach { arr: JsValue =>
      arr.asInstanceOf[JsArray].elements.map(_.asJsObject)
      .filter(_.fields.contains("event_id")).foreach { event =>
        val event_id = event.fields.get("event_id").get.toString
        event_id match {
          case "1" =>
            getStringField(event, "event_id").foreach(x => evt += "event_id" -> x)
            getStringField(event, "timestamp").foreach(x => evt += "timestamp" -> x)
            getStringField(event, "refer_page").foreach(x => evt += "refer_page" -> x)
            getStringField(event, "current_page").foreach(x => evt += "current_page" -> x)
            getStringField(event, "city_id").foreach(x => evt += "city_id" -> x)
            getStringField(event, "channel_id").foreach(x => evt += "channel_id" -> x)
            getStringField(event, "is_search").foreach(x => evt += "is_search" -> x)
            getStringField(event, "search_position").foreach(x => evt += "search_position" -> x)
            getStringField(event, "keywords").foreach(x => evt += "keywords" -> x)
            getStringField(event, "detail_id").foreach(x => evt += "detail_id" -> x)
            getStringField(event, "detail_type").foreach(x => evt += "detail_type" -> x)
            getJsObject(event, "filter_items").foreach(x => {
              val filterItems = x
            })
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_page_visits", Map("doc" -> evt)))

          case "2" =>
            getStringField(event, "timestamp").foreach(x => evt += "timestamp" -> x)
            getStringField(event, "keywords").foreach(x => evt += "keywords" -> x)
            getStringField(event, "city_id").foreach(x => evt += "city_id" -> x)
            getStringField(event, "channel_id").foreach(x => evt += "channel_id" -> x)
            getStringField(event, "filter_items").foreach(x => {
              val filterItems = x
            })
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_searches", Map("doc" -> evt)))
          case "3" =>
            getStringField(event, "attention_id").foreach(x => evt += "follow_id" -> x)
            getStringField(event, "attention_action").foreach(x => evt += "follow_action" -> x)
            getStringField(event, "attention_type").foreach(x => evt += "follow_type" -> x)
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_follows", Map("doc" -> evt)))

          case "4" =>
            getStringField(event, "timestamp").foreach(x => evt += "timestamp" -> x)
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_launches", Map("doc" -> evt)))

          case "5" =>
            getStringField(event, "timestamp").foreach(x => evt += "timestamp" -> x)
            getStringField(event, "city_id").foreach(x => evt += "city_id" -> x)
            getStringField(event, "channel_id").foreach(x => evt += "channel_id" -> x)
            getStringField(event, "current_page").foreach(x => evt += "current_page" -> x)
            getStringField(event, "duration").foreach(x => evt += "duration" -> x)
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_durations", Map("doc" -> evt)))

          case "6" =>
            getStringField(event, "timestamp").foreach(x => evt += "timestamp" -> x)
            getStringField(event, "city_id").foreach(x => evt += "city_id" -> x)
            getStringField(event, "channel_id").foreach(x => evt += "channel_id" -> x)
            getStringField(event, "current_page").foreach(x => evt += "current_page" -> x)
            getStringField(event, "event_name").foreach(x => evt += "event_name" -> x)
            reqs.addFieldScriptedUpdate(ScriptedUpdate("mobile_other_clicks", Map("doc" -> evt)))
          case _ =>
        }
      }
    }
  }

  private def getJsObject(js: JsObject, name: String): Option[JsObject] = {
    js.fields.get(name) map { x =>
      try {
        x.asJsObject
      } catch {
        case ex: Exception => null
      }
    } filter {
      _ != null
    }
  }

  private def getStringField(js: JsObject, name: String): Option[String] = {
    js.fields.get(name).filter(x => FieldUtil.isFieldValid(x.toString())).map(_.toString)
  }

  private implicit class StringUtil(line: String) {
    def removeQuotes() = line.replace("\"events\":\"[", "\"events\":[")
                         .replace("}]\"", "}]")
                         .replace("\"filter_items\":\"{", "\"filter_items\":{")
                         .replace("}\"", "}")

    def extract() = {
      val extracted = line.substring(line.indexOf("[bigdata."), line.length())
      val parts = extracted.split("#011")
      val (msgType, msg) = (parts(0), parts(2)) // todo check bound
      (msgType, msg)
    }

    def unescape() = {
      var tmp = line
      var unescaped = unescapeJava(line)
      while (unescaped != tmp) {
        tmp = unescaped
        unescaped = unescapeJava(unescaped)
      }
      unescaped
    }
  }
}
