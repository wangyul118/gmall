package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ author: Wanger
 * @ time: 2020/11/15 9:53
 * @ desc: 
 */
case class EventLog(mid: String,
                    uid: String,
                    appId: String,
                    area: String,
                    os: String,
                    logType: String,
                    eventId: String,
                    pageId: String,
                    nextPageId: String,
                    itemId: String,
                    ts: Long,
                    var logDate: String = "",
                    var logHour: String = ""
                   ) {
  val d: Date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(d)
  logHour = new SimpleDateFormat("HH").format(d)

}
