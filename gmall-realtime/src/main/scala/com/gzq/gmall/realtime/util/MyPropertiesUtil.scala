package com.gzq.gmall.realtime.util

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @Auther: gzq
 * @Date: 2021/4/7 - 04 - 07 - 21:55 
 * @Description: com.gzq.gmall.realtime.util
 */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val str = properties.getProperty("kafka.broker.list")
    println(str)

  }
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), StandardCharsets.UTF_8))
    prop
  }

}
