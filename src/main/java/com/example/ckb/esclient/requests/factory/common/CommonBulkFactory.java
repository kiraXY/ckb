package com.example.ckb.esclient.requests.factory.common;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.factory.BulkFactory;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

@Slf4j
@RequiredArgsConstructor
public final class CommonBulkFactory implements BulkFactory {
    private final ElasticSearchVersion version;

    @SneakyThrows
    @Override
    public HttpRequest bulk(ByteBuf content) {
        requireNonNull(content, "content");

        if (log.isDebugEnabled()) {
            log.debug("Bulk requests: {}", content.toString(StandardCharsets.UTF_8));
        }

        return HttpRequest.builder()
                          .post("/_bulk")
                          .content(MediaType.JSON, HttpData.wrap(content))
                          .build();
    }
}
