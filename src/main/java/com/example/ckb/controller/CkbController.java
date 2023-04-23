package com.example.ckb.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.ckb.esclient.esclient.ElasticSearchClient;
import com.example.ckb.esclient.requests.search.*;
import com.example.ckb.esclient.response.Mappings;
import com.example.ckb.esclient.response.search.SearchHit;
import com.example.ckb.esclient.response.search.SearchResponse;
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

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

@RestController
@RequestMapping()
@Slf4j
public class CkbController {

    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private ElasticSearchClient esClient;
    private ReentrantLock lock = new ReentrantLock();

    private static Map<String, Mappings> INDEXS;
    private static String SETTING = "{\"index\":{\"refresh_interval\":\"10s\",\"number_of_shards\":\"1\",\"translog\":{\"flush_threshold_size\":\"256mb\",\"sync_interval\":\"10s\"},\"number_of_replicas\":\"0\",\"merge\":{\"policy\":{\"floor_segment\":\"50mb\",\"max_merged_segment\":\"5gb\"}}}}";

    static {
        INDEXS = new HashMap<>();
        String ckbInputsMapping = "{\"@timestamp\":{\"type\":\"date\"},\"num\":{\"type\":\"long\"},\"@version\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"index\":{\"type\":\"long\"},\"txHash\":{\"type\":\"binary\"},\"since\":{\"type\":\"long\"}}";
        String ckbOutputsMapping = "{\"@timestamp\":{\"type\":\"date\"},\"num\":{\"type\":\"long\"},\"@version\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"lock\":{\"properties\":{\"args\":{\"type\":\"binary\"},\"codeHash\":{\"type\":\"binary\"},\"hashType\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}}}},\"type\":{\"properties\":{\"args\":{\"type\":\"binary\"},\"codeHash\":{\"type\":\"binary\"},\"hashType\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}}}},\"capacity\":{\"type\":\"long\"}}";
        Mappings ckbInputs = new Mappings();
        ckbInputs.setProperties(JSONObject.parseObject(ckbInputsMapping));
        Mappings ckbOutputs = new Mappings();
        ckbOutputs.setProperties(JSONObject.parseObject(ckbOutputsMapping));
        INDEXS.put("ckb_inputs", ckbInputs);
        INDEXS.put("ckb_outputs", ckbOutputs);
    }

    @PostMapping("/ckb")
    public void testString() {


        try {
            lock.lock();
            //判断kafka内数据是否消费完毕
            if (getLastDataTime("ckb_inputs") > getJobLastFinishTime()
                    || getLastDataTime("ckb_outputs") > getJobLastFinishTime()) {
                throw new RuntimeException("job 执行中");
            }

            resetESIndex();


            CkbRpcApi api = new Api("http://127.0.0.1:8114");
            LongAdder adder = new LongAdder();
            long tipBlockNumber = api.getTipBlockNumber();
            for (long i = 0; i < tipBlockNumber; i++) {
                Block block = api.getBlockByNumber(i);
                for (Transaction transaction : block.transactions) {
                    transaction.inputs.parallelStream().forEach(p -> {
                        JSONObject a = ((JSONObject) JSONObject.toJSON(p.previousOutput));
                        a.put("since", p.since);
                        adder.add(1l);
                        a.put("num", adder.longValue());
                        kafkaTemplate.send("ckb_inputs", JSONObject.toJSONString(a));
                    });
                    transaction.outputs.parallelStream().forEach(p -> {
                        JSONObject a = ((JSONObject) JSONObject.toJSON(p));
                        adder.add(1l);
                        a.put("num", adder.longValue());
                        kafkaTemplate.send("ckb_outputs", JSONObject.toJSONString(a));
                    });
                }
            }

            //保存job执行记录
            Map<String, Object> map = new HashMap<>();
            map.put("name", "ckb");
            map.put("num", adder.longValue());
            esClient.forceInsert("job", UUID.randomUUID().toString(), map);
        } catch (Exception e) {
            log.error("同步CKB block信息发生错误", e);
        } finally {
            lock.unlock();
        }
    }


    private void resetESIndex(){
        for (Map.Entry<String, Mappings> entry : INDEXS.entrySet()) {
            esClient.deleteByIndexName(entry.getKey());
        }
        for (Map.Entry<String, Mappings> entry : INDEXS.entrySet()) {
            esClient.createIndex(entry.getKey(), entry.getValue(), JSONObject.parseObject(SETTING));
        }
    }

    private long getJobLastFinishTime() {
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
