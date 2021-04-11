package com.gzq.gmall.realtime.dim


import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.gzq.gmall.realtime.bean.UserInfo
import com.gzq.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther: gzq
 * @Date: 2021/4/11 - 04 - 11 - 16:09
 * @Description: com.gzq.gmall.realtime.dim
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ods_user_info"
    val groupId: String = "user_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDstream  = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDstream  = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangeDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val userInfoDstream: DStream[UserInfo] = offsetRangeDstream.map {
      record => {
        val jsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: java.util.Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val betweenMs = curTs - date.getTime
        val age = betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20 岁及以下"
        }else if(age>30){
          userInfo.age_group="30 岁以上"
        }else{
          userInfo.age_group="21 岁到 30 岁"
        }
        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }

        userInfo
      }
    }


    import org.apache.phoenix.spark._
    userInfoDstream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix("TEST102",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("10.251.3.115,10.251.1.125,10.251.2.111:2181")
        )
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
