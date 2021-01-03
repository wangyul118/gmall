package com.atguigu.canal

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
 * @ author: Wanger
 * @ time: 2020/11/13 21:41
 * @ desc: 
 */
object MyKafkaUtil {
  val props: Properties = new Properties()
  props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

   val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def send(topic:String,context :String): Future[RecordMetadata] ={
    producer.send(new ProducerRecord[String,String](topic,context))
  }


}
