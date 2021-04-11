package com.gzq.gmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, DocumentResult, Get, Index, Search, SearchResult}
import java.util

import com.gzq.gmall.realtime.bean.DauInfo
import com.gzq.gmall.realtime.util.MyRedisUtil.getJedisClient
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
 * @Auther: gzq
 * @Date: 2021/4/7 - 04 - 07 - 20:15 
 * @Description: com.gzq.gmall.realtime
 */
object MyESUtil {
  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = _

  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {
    if (infoList != null && infoList.size > 0) {
      val jestClient = getJestClient
      val bulk: Bulk.Builder = new Bulk.Builder()
      for ((id,elem) <- infoList) {
        val index = new Index.Builder(elem)
          .index(indexName)
          .`type`("_doc")
          .id(id)
          .build()
        bulk.addAction(index)
      }
      val bulk1 = bulk.build()
      val bulkResult = jestClient.execute(bulk1)
      println("向ES中插入"+bulkResult.getItems.size()+"条数据")
      jestClient.close()
    }

  }

  def getJestClient: JestClient = {
    if (jestFactory == null) {
      build()
    }
    jestFactory.getObject
  }

  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://192.168.56.10:9200").multiThreaded(true).maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())
  }

  def putIndex(): Unit = {
    //获取客户端连接
    val client: JestClient = getJestClient
    val actorList: util.List[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actorMap1 = new util.HashMap[String, Any]()
    actorMap1.put("id", 66)
    actorMap1.put("name", "李若彤")
    actorList.add(actorMap1)
    val movie = Movie(300, "天龙", 9.0f, actorList)
    val index = new Index.Builder(movie)
      .index("movie_index_5")
      .`type`("movie")
      .id("2")
      .build()
    client.execute(index)

    client.close()
  }

  def queryIndexById(): Unit = {
    val client = getJestClient
    val get: Get = new Get.Builder("movie_index_5", "2")
      .build()
    val res: DocumentResult = client.execute(get)
    println(res.getJsonString)
    client.close()
  }

  def queryIndexByCondition(): Unit = {
    val client = getJestClient
    //    val query =
    //      """
    //        |{
    //        | "query": {
    //        | "bool": {
    //        | "must": [
    //        | {"match": {
    //        | "name": "天龙"
    //        | }}
    //        | ],
    //        | "filter": [
    //        | {"term": { "actorList.name.keyword": "李若彤"}}
    //        | ]
    //        | }
    //        | },
    //        | "from": 0,
    //        | "size": 20,
    //        | "sort": [
    //        | {
    //        | "doubanSource": {
    //        | "order": "desc"
    //        | }
    //        | }
    //        | ],
    //        | "highlight": {
    //        | "fields": {
    //        | "name": {}
    //        | }
    //        | }
    //        |}
    // """.stripMargin
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "李若彤"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanSource", SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString()

    //    println(query)
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5").build()
    val result: SearchResult = client.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val list1 = list.asScala.map(_.source).toList
    println(list1.mkString("\n"))
    client.close()
  }

  def main(args: Array[String]): Unit = {
    //    putIndex
    queryIndexByCondition()
  }

}

case class Movie(id: Long, name: String, doubanSource: Float, actorList: util.List[util.Map[String, Any]]) {}
