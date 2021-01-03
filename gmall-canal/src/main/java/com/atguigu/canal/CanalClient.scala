package com.atguigu.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.gmall.common.Constant
import com.google.protobuf.ByteString

import scala.collection.JavaConversions._
/**
 * @ author: Wanger
 * @ time: 2020/11/13 21:47
 * @ desc:
 */
object CanalClient {

  def main(args: Array[String]): Unit = {
    //1.连接到canal
//    val address: InetSocketAddress = new InetSocketAddress("hadoop102", 11111)
    val connector: CanalConnector = CanalConnectors.newClusterConnector("hadoop102:2181","example","","")
    connector.connect() //连接
    //1.1订阅数据 gmall1015.* 表示gmall1015数据下所有的表
    connector.subscribe("gmall1015.*")
    //2.读数据
    while (true){ // 2.1 使用循环的方式持续的从canal服务中读取数据
      val msg: Message = connector.get(100) // 2.2 一次从canal拉取最多100条sql引起的数据变化
      // 2.3 一个entry封装一条sql的变化结果   . 做非空的判断
      val entries: util.List[CanalEntry.Entry] = msg.getEntries

      import scala.collection.JavaConversions._

      if(entries.size() > 0){
        //2.4遍历每行数据
        for(entry <- entries){
          //2.5只对这样的EntryType做处理
          if( entry !=null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA){
            //2.6获取这行数据，但是这种数据不是字符串, 所以要解析
            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            //2.7一个storeValue中有多个RowData, 每个RowData表示一行数据的变化
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            //2.8解析rowDatas中的每行的每列的数据
            handleData(entry.getHeader.getTableName,rowDatas,rowChange.getEventType)

          }
        }
      }else {
        println("没有拉取到数据, 2s之后重新拉取")
        Thread.sleep(2000)
      }
    }

    def handleData(tableName: String,
                   rowDatas: util.List[CanalEntry.RowData],
                   eventType: CanalEntry.EventType): Unit = {
      if("order_info" == tableName && eventType == EventType.INSERT && rowDatas !=null && rowDatas.size() > 0){
        sendToKafka(Constant.TOPIC_ORDER_INFO,rowDatas)
      }else if ("order_detail" == tableName && eventType == EventType.INSERT && rowDatas != null && rowDatas.size() > 0) {
        sendToKafka(Constant.TOPIC_ORDER_DETAIL, rowDatas)
      }
    }

    def sendToKafka(topic: String, rowDatas: util.List[CanalEntry.RowData]): Unit = {
      for(rowData <- rowDatas){
        val result: JSONObject = new JSONObject()
        // 1. 一行所有的变化后的列.
        val columnlist: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        // 2. 一行数据将来在kafka中, 应该放一起. 多列中封装到一个json字符串中
        for (column <- columnlist) {
          val key : String =column.getName
          val value :String = column.getValue
          result.put(key,value)
        }
        //3.将数据写入到kafka
        MyKafkaUtil.send(topic,result.toJSONString)
        println(result.toJSONString)

      }
    }

  }
}
