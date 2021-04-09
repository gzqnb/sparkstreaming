package com.gzq.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @Auther: gzq
 * @Date: 2021/4/8 - 04 - 08 - 22:05 
 * @Description: com.gzq.gmall.realtime.util
 */
object OffsetManagerUtil {
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    var offsetKey: String = "offset:" + topic + ":" + groupId
    val offsetMap = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      val fromOffset: Long = offsetRange.fromOffset
      val partitionId: Int = offsetRange.partition
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString, untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "--->" + untilOffset)
    }
    val jedis: Jedis = MyRedisUtil.getJedisClient
    jedis.hmset(offsetKey, offsetMap)
    jedis.close()
  }

  //type:hash key:offset:topic:groupId field:partition value
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedisClient = MyRedisUtil.getJedisClient
    var offsetKey: String = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey)
    jedisClient.close()

    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        //        Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    oMap
  }

}
