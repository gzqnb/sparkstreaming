package com.gzq.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gzq.gmall.realtime.bean.{DauInfo, OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.gzq.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyKakfaSink, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther: gzq
 * @Date: 2021/4/10 - 04 - 10 - 22:24 
 * @Description: com.gzq.gmall.realtime.dwd
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"
    var recordStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size > 0) {
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val orderInfoStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
    //    orderInfoStream.print(1000)
    //判断是否为首单
    //    val orderInfoFirstFlagDstream: DStream[OrderInfo] = orderInfoStream.map {
    //      //获取用户id
    //      orderInfo => {
    //        val userId: Long = orderInfo.user_id
    //        var sql: String = s"select user_id,if_consumed from test100 where user_id='${userId}'"
    //        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
    //        if (userStatusList != null && userStatusList.size > 0) {
    //          orderInfo.if_first_order = "0"
    //        } else {
    //          orderInfo.if_first_order = "1"
    //        }
    //        orderInfo
    //      }
    //    }
    //    orderInfoFirstFlagDstream.print(1000)
    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoStream.mapPartitions {
      orderInfoItr => {
        //当前一个分区中所有订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中获取下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        var sql: String = s"select user_id,if_consumed from test100 where user_id in('${userIdList.mkString("','")}')"

        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    //    orderInfoWithFirstFlagDstream.print(1000)

    val mapDstream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo))
    val groupByKeyDstream: DStream[(Long, Iterable[OrderInfo])] = mapDstream.groupByKey()
    val orderInfoRealDstream: DStream[OrderInfo] = groupByKeyDstream.flatMap {
      case (userId, orderInfoItr) => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        if (orderInfoList != null && orderInfoList.size > 1) {
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          if (sortedOrderInfoList(0).if_first_order == "1") {
            //时间最早的订单首单状态保留为1，其他的都设置为非首单
            for (i <- 1 until sortedOrderInfoList.size) {
              sortedOrderInfoList(i).if_first_order = "0"
            }
          }
          sortedOrderInfoList
        } else {
          orderInfoList
        }
      }
    }
    //    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDstream.mapPartitions {
    //      orderInfoItr => {
    //        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
    //        //获取当前分区中订单对应的省份id
    //        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
    //        var sql: String = s"select id, name, area_code, iso_code from test101 where id in('${provinceIdList.mkString("','")}')"
    //        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
    //        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
    //          provinceJsonObj => {
    //            //            JSON.parseObject(provinceJsonObj.toString,classOf[ProvinceInfo])
    //            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
    //            (provinceInfo.id, provinceInfo)
    //          }
    //        }.toMap
    //
    //        for (orderInfo <- orderInfoList) {
    //          val proInfo: ProvinceInfo = provinceInfoMap.getOrElse(orderInfo.province_id.toString, null)
    //          if (proInfo != null) {
    //            orderInfo.province_name = proInfo.name
    //            orderInfo.province_area_code = proInfo.area_code
    //            orderInfo.province_iso_code = proInfo.iso_code
    //          }
    //        }
    //        orderInfoList.toIterator
    //      }
    //    }
    //    orderInfoWithProvinceDStream.print(1000)

    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDstream.transform {
      //在driver端执行
      rdd => {
        var sql: String = "select id, name, area_code, iso_code from test101"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        val bdMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        rdd.map {
          orderInfo => {
            val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }
    //    orderInfoWithProvinceDStream.print(1000)
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from test102 where id in ('${userIdList.mkString("','")}')"
        val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap
        for (orderInfo <- orderInfoList) {
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }

        }
        orderInfoList.toIterator
      }
    }
    orderInfoWithUserInfoDStream.print(1000)

    //维护首单用户状态
    import org.apache.phoenix.spark._
    orderInfoWithUserInfoDStream.foreachRDD {
      rdd => {
        //优化
        rdd.cache()
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        userStatusRDD.saveToPhoenix(
          "TEST100",
          Seq("USER_ID", "IF_CONSUMED"),
          new Configuration,
          Some("10.251.3.115,10.251.1.125,10.251.2.111:2181")
        )
        rdd.foreachPartition {
          orderInfoItr => {
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall_2020_order_info_" + dateStr)
            //写回kafka
            for ((orderInfoId,orderInfo) <- orderInfoList) {
              MyKakfaSink.send("dwd_order_info",JSON.toJSONString(orderInfo,new SerializeConfig(true)))


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
