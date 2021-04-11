package com.gzq.gmall.realtime.bean

/**
 * @Auther: gzq
 * @Date: 2021/4/10 - 04 - 10 - 21:55 
 * @Description: com.gzq.gmall.realtime.bean
 */
case class UserStatus(
                       userId: String, //用户 id
                       ifConsumed: String //是否消费过 0 首单 1 非首单
                     )
