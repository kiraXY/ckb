package com.example.ckb.esclient.bulk;

import com.example.ckb.esclient.elasticsearch.ElasticSearch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Slf4j
@RequiredArgsConstructor
public final class BulkProcessorBuilder {
    private int bulkActions = -1;
    private Duration flushInterval;
    private int concurrentRequests = 2;

    public BulkProcessorBuilder bulkActions(int bulkActions) {
        checkArgument(bulkActions > 0, "bulkActions must be positive");
        this.bulkActions = bulkActions;
        return this;
    }

    public BulkProcessorBuilder flushInterval(Duration flushInterval) {
        this.flushInterval = requireNonNull(flushInterval, "flushInterval");
        return this;
    }

    public BulkProcessorBuilder concurrentRequests(int concurrentRequests) {
        checkArgument(concurrentRequests >= 0, "concurrentRequests must be >= 0");
        this.concurrentRequests = concurrentRequests;
        return this;
    }

    public BulkProcessor build(AtomicReference<ElasticSearch> es) {
        return new BulkProcessor(
            es, bulkActions, flushInterval, concurrentRequests);
    }
}
