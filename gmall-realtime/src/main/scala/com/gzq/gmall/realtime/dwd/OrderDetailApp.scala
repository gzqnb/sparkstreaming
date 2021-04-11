package com.gzq.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gzq.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.gzq.gmall.realtime.util.{MyKafkaUtil, MyKakfaSink, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.SerializableConfiguration

/**
 * @Auther: gzq
 * @Date: 2021/4/11 - 04 - 11 - 18:46 
 * @Description: com.gzq.gmall.realtime.dwd
 */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"
    //从 redis 中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] =
      OffsetManagerUtil.getOffset(topic, groupId)
    //通过偏移量到 Kafka 中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc,
        offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputDstream.transform {
        rdd => {
          //周期性在 driver 中执行
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }


    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map {
      record => {
        val jsonString: String = record.value()
        //订单处理 转换成更方便操作的专用样例类
        val orderDetail: OrderDetail = JSON.parseObject(jsonString,
          classOf[OrderDetail])
        orderDetail
      }
    }
    orderDetailDstream.print(1000)


    //关联维度数据
    val orderDetailWithSkuDStream: DStream[OrderDetail] = orderDetailDstream.mapPartitions {
      orderDetailItr => {
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        //从订单明细中获取所有的商品id
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        //根据商品id到phoenix中查询所有商品
        var sql: String = s"select id, tm_id, spu_id, category3_id, tm_name, spu_name, category3_name from test106 where id in('${skuIdList.mkString("','")}')"
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuInfoMap: Map[String, SkuInfo] = skuJsonObjList.map {
          skuJsonObj => {
            val skuInfo: SkuInfo = JSON.toJavaObject(skuJsonObj, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }
        }.toMap
        for (orderDetail <- orderDetailList) {
          val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)
          if (skuInfo != null) {
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.spu_name = skuInfo.spu_name
            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.category3_name = skuInfo.category3_name
            orderDetail.tm_name = skuInfo.tm_name
          }
        }
        orderDetailList.toIterator
      }
    }

    orderDetailWithSkuDStream.print(1000)

    orderDetailDstream.foreachRDD {
      rdd => {
        rdd.foreach {
          orderDetail => {
            MyKakfaSink.send("dwd_order_detail", JSON.toJSONString(orderDetail, new SerializeConfig(true)))
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
