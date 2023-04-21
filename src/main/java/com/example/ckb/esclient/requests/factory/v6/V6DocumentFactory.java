package com.example.ckb.esclient.requests.factory.v6;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.IndexRequest;
import com.example.ckb.esclient.requests.UpdateRequest;
import com.example.ckb.esclient.requests.factory.DocumentFactory;
import com.example.ckb.esclient.requests.search.Query;
import com.google.common.collect.ImmutableMap;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpRequestBuilder;
import com.linecorp.armeria.common.MediaType;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.requireNonNull;

@Slf4j
@RequiredArgsConstructor
final class V6DocumentFactory implements DocumentFactory {
    private final ElasticSearchVersion version;

    @Override
    public HttpRequest exist(String index, String type, String id) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");

        return HttpRequest.builder()
                          .head("/{index}/{type}/{id}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .pathParam("id", id)
                          .build();
    }

    @Override
    public HttpRequest get(String index, String type, String id) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");

        return HttpRequest.builder()
                          .get("/{index}/{type}/{id}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .pathParam("id", id)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest mget(String index, String type, Iterable<String> ids) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(ids != null && !isEmpty(ids), "ids cannot be null or empty");

        final Map<String, Iterable<String>> m = ImmutableMap.of("ids", ids);
        final byte[] content = version.codec().encode(m);

        if (log.isDebugEnabled()) {
            log.debug("mget {} ids: {}", index, ids);
        }

        return HttpRequest.builder()
                          .get("/{index}/{type}/_mget")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .content(MediaType.JSON, content)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest index(IndexRequest request, Map<String, ?> params) {
        requireNonNull(request, "request");

        final String index = request.getIndex();
        final String type = request.getType();
        final String id = request.getId();
        final Map<String, ?> doc = request.getDoc();

        checkArgument(!isNullOrEmpty(index), "request.index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "request.type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "request.id cannot be null or empty");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }
        final byte[] content = version.codec().encode(doc);

        builder.put("/{index}/{type}/{id}")
               .pathParam("index", index)
               .pathParam("type", type)
               .pathParam("id", id)
               .content(MediaType.JSON, content);

        return builder.build();
    }

    @SneakyThrows
    @Override
    public HttpRequest update(UpdateRequest request, Map<String, ?> params) {
        requireNonNull(request, "request");

        final String index = request.getIndex();
        final String type = request.getType();
        final String id = request.getId();
        final Map<String, Object> doc = request.getDoc();

        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        checkArgument(!isNullOrEmpty(id), "id cannot be null or empty");
        checkArgument(doc != null && !isEmpty(doc.entrySet()), "doc cannot be null or empty");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }
        final byte[] content = version.codec().encode(ImmutableMap.of("doc", doc));

        builder.post("/{index}/{type}/{id}/_update")
               .pathParam("index", index)
               .pathParam("type", type)
               .pathParam("id", id)
               .content(MediaType.JSON, content);

        return builder.build();
    }

    @SneakyThrows
    @Override
    public HttpRequest delete(String index, String type, Query query,
                              Map<String, ?> params) {
        checkArgument(!isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!isNullOrEmpty(type), "type cannot be null or empty");
        requireNonNull(query, "query");

        final HttpRequestBuilder builder = HttpRequest.builder();
        if (params != null) {
            params.forEach(builder::queryParam);
        }

        final byte[] content = version.codec().encode(ImmutableMap.of("query", query));

        return builder.delete("/{index}/{type}/_delete_by_query")
                      .pathParam("index", index)
                      .pathParam("type", type)
                      .content(MediaType.JSON, content)
                      .build();
    }
}
