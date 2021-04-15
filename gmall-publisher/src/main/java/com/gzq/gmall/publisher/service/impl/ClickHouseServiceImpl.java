package com.gzq.gmall.publisher.service.impl;

import com.gzq.gmall.publisher.mapper.OrderWideMapper;
import com.gzq.gmall.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: gzq
 * @Date: 2021/4/15 - 04 - 15 - 12:15
 * @Description: com.gzq.gmall.publisher.service.impl
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {
    @Autowired
    OrderWideMapper orderWideMapper;
    @Override
    public BigDecimal getOrderAmountTotal(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String, BigDecimal> rsMap = new HashMap<>();
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        for (Map map : mapList) {
            rsMap.put(String.format("%02d",map.get("hr")),(BigDecimal)map.get("am"));

        }
        return null;
    }
}
