package com.example.ckb.esclient.requests.factory.v6;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.factory.IndexFactory;
import com.example.ckb.esclient.response.Mappings;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

@RequiredArgsConstructor
final class V6IndexFactory implements IndexFactory {
    private final ElasticSearchVersion version;

    @Override
    public HttpRequest exists(String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .head("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @Override
    public HttpRequest get(final String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .get("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest create(String index,
                              Mappings mappings,
                              Map<String, ?> settings) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        final ImmutableMap.Builder<String, Object> bodyBuilder = ImmutableMap.builder();
        if (mappings != null) {
            bodyBuilder.put("mappings", mappings);
        }
        if (settings != null) {
            bodyBuilder.put("settings", settings);
        }
        final ImmutableMap<String, Object> body = bodyBuilder.build();
        final byte[] content = version.codec().encode(body);

        return HttpRequest.builder()
                          .put("/{index}")
                          .pathParam("index", index)
                          .content(MediaType.JSON, content)
                          .build();
    }

    @Override
    public HttpRequest delete(String index) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");

        return HttpRequest.builder()
                          .delete("/{index}")
                          .pathParam("index", index)
                          .build();
    }

    @SneakyThrows
    @Override
    public HttpRequest putMapping(String index, String type, Mappings mapping) {
        checkArgument(!Strings.isNullOrEmpty(index), "index cannot be null or empty");
        checkArgument(!Strings.isNullOrEmpty(type), "type cannot be null or empty");

        final byte[] content = version.codec().encode(mapping);
        return HttpRequest.builder()
                          .put("/{index}/_mapping/{type}")
                          .pathParam("index", index)
                          .pathParam("type", type)
                          .content(MediaType.JSON, content)
                          .build();
    }
}
