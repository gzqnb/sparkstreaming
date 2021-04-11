package com.gzq.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gzq.gmall.realtime.util.{MyKafkaUtil, MyKakfaSink, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther: gzq
 * @Date: 2021/4/10 - 04 - 10 - 17:34 
 * @Description: com.gzq.gmall.realtime.ods
 */
object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBMaxwellApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic = "gmall_db_m"
    var groupId = "base_db_maxwell_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = null
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val JsonObjDstream: DStream[JSONObject] = offsetDstream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    JsonObjDstream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            val opType: String = jsonObj.getString("type")

            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
            if (dataJsonObj!=null && !dataJsonObj.isEmpty
              && !"delete".equals(opType)) {
              val tableName: String = jsonObj.getString("table")
              var sendTopic = "ods_" + tableName
              MyKakfaSink.send(sendTopic, dataJsonObj.toString())
            }

          }
        }
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
