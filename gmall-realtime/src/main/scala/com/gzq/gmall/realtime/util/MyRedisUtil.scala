package com.gzq.gmall.realtime.util

import com.gzq.gmall.realtime.bean.DauInfo
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Auther: gzq
 * @Date: 2021/4/7 - 04 - 07 - 22:17 
 * @Description: com.gzq.gmall.realtime.util
 */
object MyRedisUtil {


  def main(args: Array[String]): Unit = {
    val client = getJedisClient
    println(client.ping())
  }

  private var jedisPool: JedisPool = _

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  def build():Unit = {
    val prop = MyPropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)

  }
}
