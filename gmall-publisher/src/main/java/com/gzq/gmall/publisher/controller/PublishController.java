package com.gzq.gmall.publisher.controller;

import com.gzq.gmall.publisher.service.ESService;
import com.sun.javafx.collections.MappingChange;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Auther: gzq
 * @Date: 2021/4/9 - 04 - 09 - 14:45
 * @Description: com.gzq.gmall.publisher.controller
 */
@RestController
public class PublishController {

    @Autowired
    ESService esService;

    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt) {
        List<Map<String,Object>> rsList=new ArrayList<>();
        Map<String,Object> dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if(dauTotal!=null){
            dauMap.put("value",dauTotal);
        }else {
            dauMap.put("value",0L);
        }
        rsList.add(dauMap);
        Map<String,Object> newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        rsList.add(newMidMap);
        return rsList;
    }

    @RequestMapping("/realtime-hour")
    public Object realtimeHour(@RequestParam("id") String id,@RequestParam("date") String dt){
        HashMap<Object, Map<String,Long>> rsMap = new HashMap<>();
        Map<String, Long> tdMap = esService.getDauHour(dt);
        rsMap.put("today",tdMap);
        String yd = getYd(dt);
        Map<String, Long> ydMap = esService.getDauHour(yd);
        rsMap.put("yesterday",ydMap);

        return rsMap;
    }


    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }

}
