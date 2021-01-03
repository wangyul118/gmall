package com.atguigu.gmall.common

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

/**
 * @ author: Wanger
 * @ time: 2020/11/15 22:18
 * @ desc: 
 */
object ESUtil {

  val esUrl: String = "http://hadoop102:9200"
  //1.先有es的客户端
  val factory: JestClientFactory = new JestClientFactory
  val conf: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100) // 最多同时可以有100个到es的连接的客户端
    .connTimeout(10 * 1000) // 连接到es的超时时间
    .readTimeout(10 * 1000) // 读取数据的的时候的超时时间
    .multiThreaded(true)
    .build()
  factory.setHttpClientConfig(conf)

  //向ES中插入单条数据
  def insertSingle(index: String, source: Object, id: String = null): Unit = {
    //向ES中写入数据
    val client: JestClient = factory.getObject
    val action: Index = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()
    client.execute(action)
    client.shutdownClient()
  }

  //向ES中插入多条数据
  def insertBulk(index: String, source: Iterator[Any]): Unit = {
    val client: JestClient = factory.getObject

    val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      .defaultIndex(index) // 多个doc应该进入同一个index中
      .defaultType("_doc")
    source.foreach {
      case (id: String, data) => {
        val action: Index = new Index.Builder(data).id(id).build()
        bulkBuilder.addAction(action)
      }
      case data => {
        val action: Index = new Index.Builder(data).build()
        bulkBuilder.addAction(action)
      }
    }
    client.execute(bulkBuilder.build())
    client.shutdownClient()
  }

  def main(args: Array[String]): Unit = {

    val list2: Seq[(String, User)] = ("100",User(1, "a"))::("200",User(2, "b"))::("300",User(3, "c"))::Nil
    insertBulk("user", list2.toIterator)
  }

}

case class User(age: Int, name: String)