package com.example.ckb.esclient.esclient;

import cn.hutool.core.util.StrUtil;
import com.example.ckb.esclient.bulk.BulkProcessor;
import com.example.ckb.esclient.elasticsearch.*;
import com.example.ckb.esclient.requests.search.Delete;
import com.example.ckb.esclient.requests.search.Query;
import com.example.ckb.esclient.requests.search.Search;
import com.example.ckb.esclient.requests.search.SearchParams;
import com.example.ckb.esclient.response.Document;
import com.example.ckb.esclient.response.Index;
import com.example.ckb.esclient.response.Mappings;
import com.example.ckb.esclient.response.search.SearchResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * ElasticSearchClient connects to the ES server by using ES client APIs.
 */
@Slf4j
public class ElasticSearchClient {
    public static final String TYPE = "type";

    private final ElasticSearchConfig config;

    private AtomicReference<ElasticSearch> es = new AtomicReference<>();

    public ElasticSearchClient(ElasticSearchConfig config) {
        this.config = config;
        this.connect();
    }

    private void connect() {
        final ElasticSearch oldOne = es.get();

        final ElasticSearchBuilder cb =
            config.elasticSearchBuilder();

        final ElasticSearch newOne = cb.build();
        final CompletableFuture<ElasticSearchVersion> f = newOne.connect();
        f.whenComplete((ignored, exception) -> {
            if (exception != null) {
                log.error("Failed to recreate ElasticSearch client based on config", exception);
                return;
            }
            if (es.compareAndSet(oldOne, newOne)) {
                oldOne.close();
            } else {
                newOne.close();
            }
        });
        f.join();
    }

    public boolean createIndex(String indexName) {
        return createIndex(indexName, null, null);
    }

    public boolean createIndex(String indexName,
                               Mappings mappings,
                               Map<String, ?> settings) {
        return es.get().index().create(indexName, mappings, settings);
    }

    public boolean updateIndexMapping(String indexName, Mappings mapping) {
        return es.get().index().putMapping(indexName, TYPE, mapping);
    }

    public Optional<Index> getIndex(String indexName) {
        if (StrUtil.isBlank(indexName)) {
            return Optional.empty();
        }
        return es.get().index().get(indexName);
    }

    public Collection<String> retrievalIndexByAliases(String alias) {
        return es.get().alias().indices(alias).keySet();
    }

    public boolean deleteByIndexName(String indexName) {
        return es.get().index().delete(indexName);
    }

    public boolean isExistsIndex(String indexName) {
        return es.get().index().exists(indexName);
    }

    public SearchResponse search(Supplier<String[]> indices, Search search) {
        final String[] indexNames =
            Arrays.stream(indices.get())
                  .toArray(String[]::new);
        return es.get().search(
            search,
            new SearchParams()
                .ignoreUnavailable(true)
                .allowNoIndices(true)
                .expandWildcards("open"),
            indexNames);
    }

    public SearchResponse search(String indexName, Search search) {
        return es.get().search(search, indexName);
    }

    public SearchResponse search(String indexName, Search search, SearchParams params) {
        return es.get().search(search, params, indexName);
    }

    public SearchResponse delete(String indexName, Delete delete, SearchParams params) {
        return es.get().delete(delete, params, indexName);
    }

    public SearchResponse scroll(Duration contextRetention, String scrollId) {
        return es.get().scroll(contextRetention, scrollId);
    }

    public Optional<Document> get(String indexName, String id) {
        return es.get().documents().get(indexName, TYPE, id);
    }

    public boolean existDoc(String indexName, String id) {
        return es.get().documents().exists(indexName, TYPE, id);
    }

    public SearchResponse ids(String indexName, Iterable<String> ids) {
        return es.get().search(Search.builder()
                                     .size(Iterables.size(ids))
                                     .query(Query.ids(ids))
                                     .build(), indexName);
    }

    public void forceInsert(String indexName, String id, Map<String, Object> source) {
        IndexRequestWrapper wrapper = prepareInsert(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        es.get().documents().index(wrapper.getRequest(), params);
    }

    public void forceInsertThroable(String indexName, String id, Map<String, Object> source) {
        IndexRequestWrapper wrapper = prepareInsert(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        es.get().documents().indexThroable(wrapper.getRequest(), params);
    }

    public void forceUpdate(String indexName, String id, Map<String, Object> source) {
        UpdateRequestWrapper wrapper = prepareUpdate(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        es.get().documents().update(wrapper.getRequest(), params);
    }

    public IndexRequestWrapper prepareInsert(String indexName, String id,
                                             Map<String, Object> source) {
        return new IndexRequestWrapper(indexName, TYPE, id, source);
    }

    public UpdateRequestWrapper prepareUpdate(String indexName, String id,
                                              Map<String, Object> source) {
        return new UpdateRequestWrapper(indexName, TYPE, id, source);
    }

    public BulkProcessor createBulkProcessor(int bulkActions,
                                             int flushInterval,
                                             int concurrentRequests) {
        return BulkProcessor.builder()
                            .bulkActions(bulkActions)
                            .flushInterval(Duration.ofSeconds(flushInterval))
                            .concurrentRequests(concurrentRequests)
                            .build(es);
    }
}
