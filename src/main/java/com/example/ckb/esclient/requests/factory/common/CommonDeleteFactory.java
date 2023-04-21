package com.example.ckb.esclient.requests.factory.common;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.factory.DeleteFactory;
import com.example.ckb.esclient.requests.search.Delete;
import com.example.ckb.esclient.requests.search.SearchParams;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpRequestBuilder;
import com.linecorp.armeria.common.MediaType;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CommonDeleteFactory implements DeleteFactory {
    private final ElasticSearchVersion version;

    @SneakyThrows
    public HttpRequest delete(Delete delete,
                              SearchParams params,
                              String... indices) {
        final HttpRequestBuilder builder = HttpRequest.builder();

        if (indices == null || indices.length == 0) {
            log.error("不存在索引");
            return null;
        } else {
            builder.post("/{indices}/_delete_by_query?conflicts=proceed")
                    .pathParam("indices", String.join(",", indices));
        }

        if (params != null) {
            params.forEach(e -> builder.queryParam(e.getKey(), e.getValue()));
        }

        final byte[] content = version.codec().encode(delete);

        if (log.isDebugEnabled()) {
            log.debug("Delete request: {}", new String(content));
        }

        return builder
                .content(MediaType.JSON, content)
                .build();
    }
}
