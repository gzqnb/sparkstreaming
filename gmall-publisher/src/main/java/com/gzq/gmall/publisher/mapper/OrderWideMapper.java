package com.gzq.gmall.publisher.mapper;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * @Auther: gzq
 * @Date: 2021/4/14 - 04 - 14 - 22:16
 * @Description: com.gzq.gmall.publisher.mapper
 */
public interface OrderWideMapper {
    BigDecimal selectOrderAmountTotal(String date);

    List<Map> selectOrderAmountHour(String date);
}
