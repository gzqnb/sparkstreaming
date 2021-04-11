package com.gzq.gmall.realtime.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.gzq.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.gzq.gmall.realtime.util.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @Auther: gzq
 * @Date: 2021/4/11 - 04 - 11 - 20:28 
 * @Description: com.gzq.gmall.realtime.dws
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "dwd_order_info"

    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "dwd_order_detail"

    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)

    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0) {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    } else {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }

    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    } else {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }

    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
      record => {
        val orderInfoStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
        orderInfo
      }
    }

    val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //    orderInfoDStream.map(_.value()).print(1000)
    //    orderDetailDStream.map(_.value()).print(1000)
    //  以下写法错误会丢失数据
    /* val orderInfoWithKeyDStream: DStream[(Long,OrderInfo)] = orderInfoDStream.map {
       record => {
         val orderInfoJsonStr: String = record.value()
         val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
         (orderInfo.id,orderInfo)
       }
     }
     val orderDetailWithKeyDStream: DStream[(Long,OrderDetail)] = orderDetailDStream.map {
       record => {
         val orderDetailJsonStr: String = record.value()
         val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])
         (orderDetail.order_id,orderDetail)
       }
     }

     val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)*/

    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDS.window(Seconds(20), Seconds(5))

    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDS.window(Seconds(20), Seconds(5))

    // 转化成kv
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      orderInfo => {
        (orderInfo.id, orderInfo)
      }
    }
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map {
      orderDetail => {
        (orderDetail.order_id, orderDetail)
      }
    }

    //  双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)

    //  去重
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        val jedis: Jedis = MyRedisUtil.getJedisClient
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join" + orderId
          val isNotExist: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          jedis.expire(orderKey, 600)
          if (isNotExist == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        orderWideList.toIterator
      }
    }
    orderWideDStream.print(1000)

    ssc.start()
    ssc.awaitTermination()

  }

}
