package com.gzq.gmall.publisher.service;
import java.util.Map;
/**
 * @Auther: gzq
 * @Date: 2021/4/9 - 04 - 09 - 14:46
 * @Description: com.gzq.gmall.publisher.service
 */
public interface ESService {
    public Long getDauTotal(String date);
    public Map<String,Long> getDauHour(String date);
}
