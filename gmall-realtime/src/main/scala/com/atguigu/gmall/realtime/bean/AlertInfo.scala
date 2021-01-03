package com.atguigu.gmall.realtime.bean

/**
 * @ author: Wanger
 * @ time: 2020/11/15 9:55
 * @ desc: 
 */
case class AlertInfo(
                      mid: String,
                      uids: java.util.HashSet[String],
                      itemIds: java.util.HashSet[String],
                      events: java.util.List[String],
                      ts: Long
                    )
