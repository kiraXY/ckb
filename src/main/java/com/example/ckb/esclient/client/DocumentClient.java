package com.example.ckb.esclient.client;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.IndexRequest;
import com.example.ckb.esclient.requests.UpdateRequest;
import com.example.ckb.esclient.response.Document;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.Exceptions;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public final class DocumentClient {
    private final CompletableFuture<ElasticSearchVersion> version;

    private final WebClient client;

    @SneakyThrows
    public boolean exists(String index, String type, String id) {
        return version.thenCompose(
                v -> client.execute(v.requestFactory().document().exist(index, type, id))
                        .aggregate().thenApply(response -> response.status() == HttpStatus.OK)
                        .exceptionally(e -> {
                            log.error("Failed to check whether document exists", e);
                            return false;
                        })).get();
    }

    @SneakyThrows
    public Optional<Document> get(String index, String type, String id) {
        final CompletableFuture<Optional<Document>> future = version.thenCompose(
                v -> client.execute(v.requestFactory().document().get(index, type, id))
                        .aggregate().thenApply(response -> {
                            if (response.status() != HttpStatus.OK) {
                                throw new RuntimeException(response.contentUtf8());
                            }

                            try (final HttpData content = response.content();
                                 final InputStream is = content.toInputStream()) {
                                final Document document = v.codec().decode(is, Document.class);
                                if (!document.isFound()) {
                                    return Optional.empty();
                                }
                                return Optional.of(document);
                            } catch (Exception e) {
                                return Exceptions.throwUnsafely(e);
                            }
                        }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to get doc by id {} in index {}", id, index, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Doc by id {} in index {}: {}", id, index, result);
            }
        });
        return future.get();
    }

    @SneakyThrows
    public void index(IndexRequest request, Map<String, Object> params) {
        final CompletableFuture<Void> future = version.thenCompose(
                v -> client.execute(v.requestFactory().document().index(request, params))
                        .aggregate().thenAccept(response -> {
                            final HttpStatus status = response.status();
                            if (status != HttpStatus.CREATED && status != HttpStatus.OK) {
                                throw new RuntimeException(response.contentUtf8());
                            }
                        }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to index doc: {}, params: {}", request, params, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded indexing doc: {}, params: {}", request, params);
            }
        });
        future.join();
    }

    public void indexThroable(IndexRequest request, Map<String, Object> params) {

        final CompletableFuture<Void> future = version.thenCompose(
                v -> client.execute(v.requestFactory().document().index(request, params))
                        .aggregate().thenAccept(response -> {
                            final HttpStatus status = response.status();
                            if (status != HttpStatus.CREATED && status != HttpStatus.OK) {
                                throw new RuntimeException(response.contentUtf8());
                            }
                        }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to index doc: {}, params: {}", request, params, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded indexing doc: {}, params: {}", request, params);
            }
        });
        future.join();
    }

    @SneakyThrows
    public void update(UpdateRequest request, Map<String, Object> params) {
        final CompletableFuture<Void> future = version.thenCompose(
                v -> client.execute(v.requestFactory().document().update(request, params))
                        .aggregate().thenAccept(response -> {
                            final HttpStatus status = response.status();
                            if (status != HttpStatus.OK) {
                                throw new RuntimeException(response.contentUtf8());
                            }
                        }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to update doc: {}, params: {}", request, params, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded updating doc: {}, params: {}", request, params);
            }
        });
        future.join();
    }
}
