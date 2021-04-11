package com.gzq.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @Auther: gzq
 * @Date: 2021/4/10 - 04 - 10 - 21:57 
 * @Description: com.gzq.gmall.realtime.util
 */
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    var sql:String = "select * from test100"
    val rs: List[JSONObject] = queryList(sql)
    println(rs)
  }
  def queryList(sql: String): List[JSONObject] = {
    val rsList = new ListBuffer[JSONObject]
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:10.251.3.115,10.251.1.125,10.251.2.111:2181")
    val ps: PreparedStatement = conn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      val userStatusJsonObj = new JSONObject()
      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusJsonObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }

}
