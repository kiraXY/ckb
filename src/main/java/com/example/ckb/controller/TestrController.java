package com.example.ckb.controller;

import cn.hutool.json.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.ckb.esclient.esclient.ElasticSearchClient;
import com.example.ckb.esclient.requests.search.*;
import com.example.ckb.esclient.response.Mappings;
import com.example.ckb.esclient.response.search.SearchHit;
import com.example.ckb.esclient.response.search.SearchResponse;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.nervos.ckb.CkbRpcApi;
import org.nervos.ckb.service.Api;
import org.nervos.ckb.type.Block;
import org.nervos.ckb.type.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

@RestController
@RequestMapping("/test")
@Slf4j
public class TestrController {

    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private ElasticSearchClient esClient;
    private ReentrantLock lock = new ReentrantLock();

    private static String indexs = "ckb_inputs,ckb_outputs";

    @PostMapping("/ckb")
    public void testString() {


        try {
            lock.lock();
            //判断kafka内数据是否消费完毕
            if(getLastDataTime("ckb_inputs")>getJobLastFinishTime()
            ||getLastDataTime("ckb_outputs")>getJobLastFinishTime()){
                throw new RuntimeException("job 执行中");
            }
            CkbRpcApi api = new Api("http://127.0.0.1:8114");
            for (String s : indexs.split(",")) {
                esClient.deleteByIndexName(s);
            }
            for (String s : indexs.split(",")) {
                esClient.createIndex(s);
            }
            LongAdder adder=new LongAdder();
            long tipBlockNumber = api.getTipBlockNumber();
            for (long i = 0; i < 6; i++) {
                Block block = api.getBlockByNumber(i);
                for (Transaction transaction : block.transactions) {
                    transaction.inputs.forEach(p -> {
                        JSONObject a = ((JSONObject) JSONObject.toJSON(p.previousOutput));
                        a.put("since", p.since);
                        adder.add(1l);
                        a.put("num", adder.longValue());
                        kafkaTemplate.send("ckb_inputs", JSONObject.toJSONString(a));
                    });
                    transaction.outputs.forEach(p -> {
                        JSONObject a = ((JSONObject) JSONObject.toJSON(p));
                        adder.add(1l);
                        a.put("num", adder.longValue());
                        kafkaTemplate.send("ckb_outputs", JSONObject.toJSONString(a));
                    });
                }
            }

            //保存job执行记录
            Map<String,Object> map   =new HashMap<>();
            map.put("name","ckb");
            map.put("num",adder.longValue());
            esClient.forceInsert("job", UUID.randomUUID().toString(),map);
        } catch (Exception e) {
            log.error("同步CKB block信息发生错误", e);
        }finally {
            lock.unlock();
        }
    }

    private long getJobLastFinishTime(){
        SearchBuilder searchSourceBuilder = Search.builder();
        BoolQueryBuilder queryBuilder = Query.bool();
        queryBuilder.must(Query.term("name", "ckb"));
        searchSourceBuilder.sort("num", Sort.Order.DESC);
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1);
        SearchResponse response = esClient.search("job", searchSourceBuilder.build());

        final List<SearchHit> hits = response.getHits().getHits();
        if (CollectionUtils.isEmpty(hits)) {
            return 0L;
        } else {
            return ((Number) hits.get(0).getSource().get("num")).longValue();
        }
    }


    private long getLastDataTime(String indexName) {
        SearchBuilder searchSourceBuilder = Search.builder();
        BoolQueryBuilder queryBuilder = Query.bool();
        searchSourceBuilder.sort("num", Sort.Order.DESC);
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1);
        SearchResponse response = esClient.search(indexName, searchSourceBuilder.build());

        final List<SearchHit> hits = response.getHits().getHits();
        if (CollectionUtils.isEmpty(hits)) {
            return 0L;
        } else {
            return ((Number) hits.get(0).getSource().get("num")).longValue();
        }
    }

}
