package com.gzq.gmall.realtime.bean

/**
 * @Auther: gzq
 * @Date: 2021/4/8 - 04 - 08 - 14:44 
 * @Description: com.gzq.gmall.realtime.bean
 */


  case class DauInfo(
                      mid: String, //设备 id
                      uid: String, //用户 id
                      ar: String, //地区
                      ch: String, //渠道
                      vc: String, //版本
                      var dt: String, //日期
                      var hr: String, //小时
                      var mi: String, //分钟
                      ts: Long //时间戳
                    ) {}


