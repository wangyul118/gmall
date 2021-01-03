package com.atguigu.gmall.realtime.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.{Constant, ESUtil}
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import com.jcraft.jsch.UserInfo
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
 * @author: Wanger
 * @time: 2020/11/17 9:25
 * @desc:
 */
object SaleDetailApp2 {

  /**
   * 缓存入redis
   *
   * @param client
   * @param key
   * @param value
   * @return
   */
  def saveToRedis(client: Jedis, key: String, value: AnyRef) = {
    import org.json4s.DefaultFormats
    val json: String = Serialization.write(value)(DefaultFormats)
    client.setex(key, 60 * 30, json)
  }

  /**
   * 缓存cacheOrderInfo
   *
   * @param client
   * @param orderInfo
   */
  def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo): Unit = {
    val key: String = "order_info:" + orderInfo.id
    saveToRedis(client, key, orderInfo)
  }

  /**
   * 缓存orderDetail
   *
   * @param client
   * @param orderDetail
   */
  def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail): Unit = {
    val key: String = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
    saveToRedis(client, key, orderDetail)
  }

  /**
   * 对双流进行fulljoin
   *
   * @param orderInfoDStream
   * @param orderDetailDStream
   * @return
   */
  def fullJoin(orderInfoDStream: DStream[(String, OrderInfo)],
               orderDetailDStream: DStream[(String, OrderDetail)]): DStream[SaleDetail] = {
    orderInfoDStream.fullOuterJoin(orderDetailDStream).mapPartitions(it => {
      //1.获取reids客户端
      val client: Jedis = RedisUtil.getClient
      //2.对各种延迟做处理
      val result: Iterator[SaleDetail] = it.flatMap {
        //A.orderInfocun存在，而orderDetail分两种情况
        case (orderId, (Some(orderInfo), opt)) =>
          //1.写缓存
          cacheOrderInfo(client, orderInfo)
          //2.不管opt是some还是none, 总是要去读OrderDetail的缓冲区
          val keys: List[String] = client.keys(s"order_detail:$orderId:*").toList
          //3.缓冲区集合中会有多个OrderDetail
          keys.map(key => {
            val orderDetailString: String = client.get(key)
            client.del(key)
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          }) ::: (opt match {
            case Some(orderDetail) => SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
            case None => Nil
          })

        //B.orderInfo不存在
        case (orderId, (None, Some(orderDetail))) =>
          // 1. 根据orderDetail中的orderId去缓存读取对应的orderInfo信息
          val orderInfoString: String = client.get("order_info:" + orderId)
          // 2. 读取之后, 有可能读到对应的OderInfo信息, 也有可能没有读到. 分别处理
          // 2.1 读到, 把数据封装SaleDetail中去
          if (orderInfoString != null && orderInfoString.nonEmpty) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
          } else {
            //读不到，把orderDetail写入缓存
            cacheOrderDetail(client, orderDetail)
            Nil
          }
      }
      //3.关闭客户端
      client.close()

      //4.返回处理后的结果
      result

    })
  }

  /**
   * 使用spark-sql去读mysql中的数据, 然后把需要的字段添加到 SaleDetail
   *
   * @param saleDetailDStream
   * @param ssc
   */
  def joinUser(saleDetailDStream: DStream[SaleDetail], ssc: StreamingContext) = {
    val url: String = "jdbc:mysql://hadoop102:3306/gmall1015"
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "000000")
    val spark: SparkSession = SparkSession
      .builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()
    import spark.implicits._
    // 1. 先把mysql数据读进来 每隔3s读一次
    saleDetailDStream.transform((SaleDetailRDD: RDD[SaleDetail]) => {
      /*
          2. 读mysql数据的时候, 有两种读法:
            2.1 直接会用rdd的join完成
            2.2 每个分区自己负责join(map端join)  参考map端的join代码
       */
      // 2.1 直接会用rdd的join完成
      val userInfoRDD: RDD[(String, UserInfo)] = spark
        .read
        .jdbc(url, "user_info", props)
        .as[UserInfo]
        .rdd
        .map(user => (user.id, user))

      // 2.2 两个RDD做join
      SaleDetailRDD
        .map(saleDetail => (saleDetail.user_id, saleDetail))
        .join(userInfoRDD)
        .map {
          case (_, (saleDetail, userInfo)) =>
            saleDetail.mergeUserInfo(userInfo)

        }
    })

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp2")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //1.读取kafka里面的两个topic，得到两个流
    //2.对两个流进行封装
    val orderInfoDStream: DStream[(String, OrderInfo)] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO)
      .map(s => {
        val orderInfo: OrderInfo = JSON.parseObject(s._2, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
      })

    val orderDetailDStream: DStream[(String, OrderDetail)] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
      .map(s => {
        val orderDetail: OrderDetail = JSON.parseObject(s._2, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

    //3.双流join
    val saleDetailDStream: DStream[SaleDetail] = fullJoin(orderInfoDStream, orderDetailDStream)

    //4.根据用户的id反查mysql里面的user_info表，得到用户的生日和性别
    val saleDetailStream = joinUser(saleDetailDStream, ssc)

    //5.把详情写入到es中
    ESUtil.insertBulk(Constant.INDEX_SALE_DETAIL, rdd.collect().toIterator)


    ssc.start()
    ssc.awaitTermination()


  }
}
