package com.gzq.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.gzq.gmall.realtime.bean.ProvinceInfo
import com.gzq.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther: gzq
 * @Date: 2021/4/11 - 04 - 11 - 15:40 
 * @Description: com.gzq.gmall.realtime.dim
 */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size > 0) {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    import org.apache.phoenix.spark._
    offsetDstream.foreachRDD {
      rdd => {
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            val jsonStr: String = record.value()
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            provinceInfo
          }
        }
        provinceInfoRDD.saveToPhoenix("TEST101",
          Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
          new Configuration,
          Some("10.251.3.115,10.251.1.125,10.251.2.111:2181")
        )

        //保存偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
