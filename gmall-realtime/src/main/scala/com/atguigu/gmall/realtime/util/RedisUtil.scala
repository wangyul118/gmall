package com.atguigu.gmall.realtime.util

import redis.clients.jedis.Jedis

/**
 * @ author: Wanger
 * @ time: 2020/11/13 19:17
 * @ desc: 
 */
object RedisUtil {
  val host: String = PropertiesUtil.getProperty("redis.host")
  val port: Int = PropertiesUtil.getProperty("redis.port").toInt

  def getClient: Jedis = {
    val client: Jedis = new Jedis(host, port, 60 * 1000)
    client.connect()
    client
  }
}
