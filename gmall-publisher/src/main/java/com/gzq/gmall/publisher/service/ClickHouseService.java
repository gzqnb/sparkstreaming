package com.gzq.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @Auther: gzq
 * @Date: 2021/4/15 - 04 - 15 - 12:11
 * @Description: com.gzq.gmall.publisher.service
 */
public interface ClickHouseService {
    BigDecimal getOrderAmountTotal(String date);
    Map<String,BigDecimal> getOrderAmountHour(String date);
}
