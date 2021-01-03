package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.{Constant, ESUtil}
import com.atguigu.gmall.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * @ author: Wanger
 * @ time: 2020/11/15 10:02
 * @ desc: 
 */
object AlertApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //1.先从kafka消费数据
    val inputStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT)
    val sourceStream: DStream[String] = inputStream.map(line => line._2)
    //2.数据封装
    val EventlogStream: DStream[EventLog] = sourceStream.window(Minutes(5), Seconds(6)) //添加窗口
      .map(str => JSON.parseObject(str, classOf[EventLog]))
    //    EventlogStream.print(1000)
    //EventLog(mid_35,5168,gmall,guangdong,ios,event,clickItem,17,42,2,1605409515021,2020-11-15,11)

    //3.实现需求
    /**
     * 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
     * 并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
     * 同一设备，每分钟只记录一次预警。
     *
     * 1.按照设备id进行分组
     * 2.三次及三次以上用不同账号登陆：统计同一设备登陆的用户数
     * 3.领取优惠券
     * 4.领券过程中没有浏览商品
     * 5.产生预警信息
     *
     */
    //3.1按照设备id进行分组
    val eventLogGroupDstream: DStream[(String, Iterable[EventLog])] = EventlogStream
      .map(evenlog => (evenlog.mid, evenlog))
      .groupByKey()
    //3.2产生预警信息
    val alertInfoStream: DStream[(Boolean, AlertInfo)] = eventLogGroupDstream.map {
      case (mid, eventlogs) =>
        //a:统计在当前设备(mid), 最近5分钟登录过的领取优惠券的用户id
        val uidSet: util.HashSet[String] = new util.HashSet[String]()
        //b:存储5分钟内发生的所有事件，
        val eventList: util.ArrayList[String] = new util.ArrayList[String]()
        //c存储领取了优惠券对应的那些商品的id
        val itemSet: util.HashSet[String] = new util.HashSet[String]()

        //是否点击了商品，默认是没有
        var isClickItem: Boolean = false

        breakable {
          eventlogs.foreach(log => {
            eventList.add(log.eventId) //存储所有事件
            log.eventId match {
              case "coupon" =>
                uidSet.add(log.uid)
                itemSet.add(log.itemId)
              case "clickItem" =>
                isClickItem = true //只要有一次浏览商品，就不应该产生报警信息
                break
              case _ => //其他事件忽略
            }
          })
        }
        //(boolean,预警信息)
        (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }
    //    alertInfoStream.print(1000)

    //4.把预警信息写入ES
    alertInfoStream
      .filter(_._1) //先把需要写入到es的预警信息过滤出来
      .map(_._2) //只保留预警信息
      .foreachRDD(
        rdd => {
          rdd.foreachPartition(alterinfo => {
            // 连接到es
            // 写数据
            // 关闭到es的连接
            /*
                6. 同一设备，每分钟只记录一次预警。
                    spark-steaming不实现, 交个es自己来实现
                        es中, id没分钟变化一次.
             */
            val data: Iterator[(String, AlertInfo)] = alterinfo
              .map(info => {
                (info.mid + ":" + info.ts / 1000 / 60, info)
              })
            ESUtil.insertBulk(Constant.INDEX_ALTER, data)
          })
        }
      )
    alertInfoStream.print(1000)
    ssc.start()
    ssc.awaitTermination()

  }
}
