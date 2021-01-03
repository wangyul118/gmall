package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @ author: Wanger
 * @ time: 2020/11/13 19:23
 * @ desc: 
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //1.从kafka消费数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val sourceStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)

    //2.使用redis进行数据清洗
    //2.1对数据进行封装
    val startLogDStream: DStream[StartupLog] = sourceStream.map {
      case (_, log) => JSON.parseObject(log, classOf[StartupLog])
    }
    //2.2写入之前先做过滤
    val firstStartupLogStream: DStream[StartupLog] = startLogDStream.transform {
      rdd => {
        val client: Jedis = RedisUtil.getClient
        val key: String = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MMMM-dd").format(new Date())
        val mids: util.Set[String] = client.smembers(key)
        client.close()
        //2.3把已经启动的设备过滤掉，redis中只留下那些在redis中不存在的记录
        val midBD: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
        // 2.4 考虑到某个mid在一个批次内启动了多次(而且是这个mid第一次启动), 会出现重复情况
        rdd.filter(log => !midBD.value.contains(log.mid))
          .map {
            log => {
              (log.mid, log)
            }
          }
          .groupByKey()
          .map {
            case (_, it) => it.toList.minBy(_.ts)
          }
      }
    }

    import org.apache.phoenix.spark._
    //2.4把第一次启动的设备保存到redis中
    firstStartupLogStream.foreachRDD { rdd => {
      rdd.foreachPartition(logIt => {
        //获取连接
        val client: Jedis = RedisUtil.getClient
        logIt.foreach(log => {
          //每次向set中存入一个mid
          client.sadd(Constant.TOPIC_STARTUP + ":" + log.logDate, log.mid)
        })
        client.close()
      })

      //3.写入到hbase中
      rdd.saveToPhoenix(Constant.DAU_TABLE,
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    }
    }
    firstStartupLogStream.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
