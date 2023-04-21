package com.example.ckb.esclient.esclient;

import cn.hutool.core.collection.CollUtil;
import com.example.ckb.esclient.bulk.BulkProcessor;
import com.example.ckb.esclient.elasticsearch.IndexRequestWrapper;
import com.example.ckb.esclient.elasticsearch.RequestWrapper;
import com.example.ckb.esclient.elasticsearch.UpdateRequestWrapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Data
@Slf4j
public class BatchProcessClient {
    private BulkProcessor bulkProcessor;
    private int bulkActions;
    private int flushInterval;
    private int concurrentRequests;
    private ElasticSearchClient client;

    public BatchProcessClient(ElasticSearchClient client,
                              int bulkActions,
                              int flushInterval,
                              int concurrentRequests) {
        this.client = client;
        this.bulkActions = bulkActions;
        this.flushInterval = flushInterval;
        this.concurrentRequests = concurrentRequests;
        this.bulkProcessor = client.createBulkProcessor(bulkActions, flushInterval, concurrentRequests);
    }

    public void insert(IndexRequestWrapper insertRequest) {
        if (bulkProcessor == null) {
            this.bulkProcessor = client.createBulkProcessor(bulkActions, flushInterval, concurrentRequests);
        }

        this.bulkProcessor.add(insertRequest.getRequest());
    }

    public CompletableFuture<Void> flush(List<RequestWrapper> prepareRequests) {
        if (bulkProcessor == null) {
            this.bulkProcessor = client.createBulkProcessor(bulkActions, flushInterval, concurrentRequests);
        }

        if (CollUtil.isNotEmpty(prepareRequests)) {
            return CompletableFuture.allOf(prepareRequests.stream().map(prepareRequest -> {
                if (prepareRequest instanceof RequestWrapper) {
                    return bulkProcessor.add(((IndexRequestWrapper) prepareRequest).getRequest());
                } else {
                    return bulkProcessor.add(((UpdateRequestWrapper) prepareRequest).getRequest());
                }
            }).toArray(CompletableFuture[]::new));
        }
        return CompletableFuture.completedFuture(null);
    }
}
