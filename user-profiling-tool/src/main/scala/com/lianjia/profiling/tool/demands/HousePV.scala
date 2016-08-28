package com.lianjia.profiling.tool.demands

import com.lianjia.profiling.common.elasticsearch.ESClient
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.search.{SearchAction, SearchRequestBuilder}

/**
  * @author fenglei@lianjia.com on 2016-06
  */

object HousePV extends App with ESClient {
  val hdfsUri = "hdfs://jx-bd-hadoop00.lianjia.com:9000"
  val query =
    """
      |{
      |		"bool": {
      |			"must": {
      |				"bool": {
      |					"must": [
      |						{
      |							"match": {
      |								"_type": {
      |									"query": "dtl",
      |									"type": "phrase"
      |								}
      |							}
      |						},
      |						{
      |							"match": {
      |								"dtl_id": {
      |									"query": "%house_id",
      |									"type": "phrase"
      |								}
      |							}
      |						},
      |						{
      |							"match": {
      |								"dtl_type": {
      |									"query": 1,
      |									"type": "phrase"
      |								}
      |							}
      |						}
      |					]
      |				}
      |			}
      |		}
      |	},
      |	"_source": {
      |		"includes": [
      |			"COUNT"
      |		],
      |		"excludes": []
      |	},
      |	"aggregations": {
      |		"COUNT(*)": {
      |			"value_count": {
      |				"field": "_index"
      |			}
      |		}
      |}
    """.stripMargin

  val conf = new SparkConf().setAppName("house_pv")//.setMaster("local[1]")
  val sc = new SparkContext(conf)

  // val data = sc.textFile(args(0))
  // val data = sc.parallelize(Array("TJHD90441573"))
  val data = sc.textFile("/user/bigdata/profiling/demands/house_codes_bj")

  data mapPartitions { part =>
    val client = getClient
    part map { houseId =>
      val resp = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                 .setIndices("online_user_20160613", "online_user_20160606")
                 .setQuery(query.replace("%house_id", houseId))
                 .setSize(0)
                 .get
      s"$houseId\t${resp.getHits.totalHits()}"
    }
  } saveAsTextFile (hdfsUri + "/user/bigdata/profiling/demands/house_pv")

  sc.stop()
}
