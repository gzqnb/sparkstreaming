package com.gzq.gmall.realtime.bean

/**
 * @Auther: gzq
 * @Date: 2021/4/11 - 04 - 11 - 15:37 
 * @Description: com.gzq.gmall.realtime.bean
 */
case class UserInfo(
                     id: String,
                     user_level: String,
                     birthday: String,
                     gender: String,
                     var age_group: String, //年龄段
                     var gender_name: String
                   )
