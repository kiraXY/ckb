package com.example.ckb.esclient.client;

import com.example.ckb.esclient.elasticsearch.ElasticSearchVersion;
import com.example.ckb.esclient.requests.search.Delete;
import com.example.ckb.esclient.requests.search.Scroll;
import com.example.ckb.esclient.requests.search.Search;
import com.example.ckb.esclient.requests.search.SearchParams;
import com.example.ckb.esclient.response.search.SearchResponse;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.Exceptions;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public final class SearchClient {
    private final CompletableFuture<ElasticSearchVersion> version;

    private final WebClient client;

    @SneakyThrows
    public SearchResponse search(Search criteria,
                                 SearchParams params,
                                 String... index) {
        final CompletableFuture<SearchResponse> future =
                version.thenCompose(
                        v -> client.execute(v.requestFactory().search().search(criteria, params, index))
                                .aggregate().thenApply(response -> {
                                    if (response.status() != HttpStatus.OK) {
                                        throw new RuntimeException(response.contentUtf8());
                                    }

                                    try (final HttpData content = response.content();
                                         final InputStream is = content.toInputStream()) {
                                        return v.codec().decode(is, SearchResponse.class);
                                    } catch (Exception e) {
                                        return Exceptions.throwUnsafely(e);
                                    }
                                }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error(
                        "Failed to search, request {}, params {}, index {}",
                        criteria, params, index,
                        exception
                );
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to search index {}, {}", index, result);
            }
        });
        return future.get();
    }

    @SneakyThrows
    public SearchResponse scroll(Scroll scroll) {
        final CompletableFuture<SearchResponse> future =
                version.thenCompose(
                        v -> client.execute(v.requestFactory().search().scroll(scroll))
                                .aggregate().thenApply(response -> {
                                    if (response.status() != HttpStatus.OK) {
                                        throw new RuntimeException(response.contentUtf8());
                                    }

                                    try (final HttpData content = response.content();
                                         final InputStream is = content.toInputStream()) {
                                        return v.codec().decode(is, SearchResponse.class);
                                    } catch (Exception e) {
                                        return Exceptions.throwUnsafely(e);
                                    }
                                }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to scroll, request {}, {}", scroll, exception);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to scroll, {}", result);
            }
        });
        return future.get();
    }

    @SneakyThrows
    public SearchResponse delete(Delete criteria,
                                 SearchParams params,
                                 String... index) {
        final CompletableFuture<SearchResponse> future =
                version.thenCompose(
                        v -> client.execute(v.requestFactory().delete().delete(criteria, params, index))
                                .aggregate().thenApply(response -> {
                                    if (response.status() != HttpStatus.OK) {
                                        throw new RuntimeException(response.contentUtf8());
                                    }

                                    try (final HttpData content = response.content();
                                         final InputStream is = content.toInputStream()) {
                                        return v.codec().decode(is, SearchResponse.class);
                                    } catch (Exception e) {
                                        return Exceptions.throwUnsafely(e);
                                    }
                                }));
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error(
                        "Failed to delete, request {}, params {}, index {}",
                        criteria, params, index,
                        exception
                );
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to search index {}, {}", index, result);
            }
        });
        return future.get();
    }
}
