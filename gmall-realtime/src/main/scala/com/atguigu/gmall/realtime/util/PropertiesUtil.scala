package com.atguigu.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

/**
 * @ author: Wanger
 * @ time: 2020/11/13 18:53
 * @ desc: 
 */
object PropertiesUtil {
  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties: Properties = new Properties()
  properties.load(is)

  def getProperty(propertyName : String):String = properties.getProperty(propertyName)

  def main(args: Array[String]): Unit = {
    println(getProperty("kafka.broker.list"))
  }

}
