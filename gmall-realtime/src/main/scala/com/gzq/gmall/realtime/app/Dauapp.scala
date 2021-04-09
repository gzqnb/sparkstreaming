package com.gzq.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gzq.gmall.realtime.bean.DauInfo
import com.gzq.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @Auther: gzq
 * @Date: 2021/4/7 - 04 - 07 - 22:28 
 * @Description: com.gzq.gmall.realtime.app
 */
object Dauapp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic = "start1"
    var groupId: String = "test-000"

    val offsetMap = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDstream
        = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前采集周期从kafka中消费的数据的起始偏移量以及结束偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val jsonObjDstream: DStream[JSONObject] = offsetDstream.map { record => {
      val jsonString = record.value()
      //将json字符串转化为json对象
      val jsonObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObject.getLong("ts")
      //将时间戳转换为日期和小时
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateStrArr: Array[String] = dateStr.split(" ")
      val dt = dateStrArr(0)
      val hr = dateStrArr(1)
      jsonObject.put("dt", dt)
      jsonObject.put("hr", hr)
      jsonObject
    }
    }
    //    jsonObjDstream.print(1000)
    //对采集到的数据进行去重操作
    //    val filteredDstream = jsonObjDstream.filter({
    //      jsonObj => {
    //        val dt = jsonObj.getString("dt")
    //        //获取设备id
    //        val mid = jsonObj.getJSONObject("common").getString("mid")
    //        var dauKey: String = "dau:" + dt
    //
    //        val jedis = MyRedisUtil.getJedisClient
    //        val isFirst = jedis.sadd(dauKey, mid)
    //        //设置失效时间
    //        if (jedis.ttl(dauKey) < 0) {
    //          jedis.expire(dauKey, 3600 * 24)
    //        }
    //        jedis.close()
    //        if (isFirst == 1L) {
    //          true
    //        } else {
    //          false
    //        }
    //      }
    //    })
    //花括号可以进行模式匹配
    val filteredDstream = jsonObjDstream.mapPartitions {
      jsonObjItr => { //以分区为单位获取连接
        val jedis = MyRedisUtil.getJedisClient
        //定义一个集合采集第一次登录的日志
        val filteredList = new ListBuffer[JSONObject]
        for (jsonObj <- jsonObjItr) {
          //获取日期和设备id
          val dt = jsonObj.getString("dt")
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val dauKey: String = "dau:" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          if (isFirst == 1L) {
            //是第一次登陆
            filteredList.append(jsonObj)
          }
          //设置失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
        }

        jedis.close()
        //mappartitions必须返回一个可迭代对象
        filteredList.toIterator
      }
    }
    //    filteredDstream.count().print()

    //将数据批量保存到es中
    filteredDstream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          jsonObjItr => {
            val dauInfoList:List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall2020_dau_info_" + dt)
          }

        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }



    ssc.start()
    ssc.awaitTermination()


  }

}
