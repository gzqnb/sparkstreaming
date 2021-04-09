package com.gzq.gmall.publisher.service.impl;

import com.google.gson.JsonElement;
import com.gzq.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.management.Query;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: gzq
 * @Date: 2021/4/9 - 04 - 09 - 14:46
 * @Description: com.gzq.gmall.publisher.service.impl
 */
@Service
public class ESServiceImpl implements ESService {

    @Autowired
    JestClient jestClient;


    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        String indexName = "gmall2020_dau_info_" + date + "-query";
        Search search = new
                Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
//            if (searchResult.getTotal() != null) {
//                total = searchResult.getTotal();
//            }
            total = 60L;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询 ES 异常");
        }
        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        HashMap<String, Long> hourMap = new HashMap<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggragationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);
        sourceBuilder.aggregation(termsAggragationBuilder);
        String indexName = "gmall2020_dau_info_" + date + "-query";

        String query = sourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation termsAgg = result.getAggregations().getTermsAggregation("groupBy_hr");
            if (termsAgg != null) {
                List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(), bucket.getCount());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(("es查询异常"));
        }
        return hourMap;
    }
}
