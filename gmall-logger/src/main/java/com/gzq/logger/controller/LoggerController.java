package com.gzq.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @Auther: gzq
 * @Date: 2021/4/7 - 04 - 07 - 18:31
 * @Description: com.gzq.logger.controller
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog){
        log.info(mockLog);
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if (startJson!=null){
            //启动日志
//            new KafkaProducer<String,String>().send(new ProducerRecord<>())
            kafkaTemplate.send("start1",mockLog);
        }else {
            //事件日志
            kafkaTemplate.send("event1",mockLog);
        }
        return "success";
    }
}
