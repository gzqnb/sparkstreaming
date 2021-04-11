package com.gzq.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gzq.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.gzq.gmall.realtime.util.MyKakfaSink;

/**
 * @Auther: gzq
 * @Date: 2021/4/10 - 04 - 10 - 16:39
 * @Description: com.gzq.gmall.realtime
 */
object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "start3"
    var groupId = "base_db_canal_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val jsonObjectDstream: DStream[JSONObject] = offsetDstream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    jsonObjectDstream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            val onType: String = jsonObj.getString("type")
            if ("INSERT".equals(onType)) {
              val tableName: String = jsonObj.getString("table")
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              var sendTopic = "ods_" + tableName
              import scala.collection.JavaConverters._
              for (data <- dataArr.asScala) {
                MyKakfaSink.send(sendTopic, data.toString)
              }
            }


          }
        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
