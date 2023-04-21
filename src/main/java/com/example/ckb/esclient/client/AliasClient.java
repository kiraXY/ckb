package com.example.ckb.esclient.client;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.response.Index;
import com.fasterxml.jackson.core.type.TypeReference;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.Exceptions;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public final class AliasClient {
    private final CompletableFuture<ElasticSearchVersion> version;

    private final WebClient client;

    @SneakyThrows
    public Map<String, Index> indices(String name) {
        final CompletableFuture<Map<String, Index>> future =
            version.thenCompose(
                v -> client.execute(v.requestFactory().alias().indices(name))
                           .aggregate().thenApply(response -> {
                        final HttpStatus status = response.status();
                        if (status != HttpStatus.OK) {
                            throw new RuntimeException(response.contentUtf8());
                        }

                        try (final HttpData content = response.content();
                             final InputStream is = content.toInputStream()) {
                            return v.codec().decode(is, new TypeReference<Map<String, Index>>() {
                            });
                        } catch (Exception e) {
                            return Exceptions.throwUnsafely(e);
                        }
                    }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to get indices by alias {}.", name, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Indices by alias {}: {}", name, result);
            }
        });
        return future.get();
    }
}
