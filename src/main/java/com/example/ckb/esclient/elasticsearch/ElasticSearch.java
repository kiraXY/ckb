package com.example.ckb.esclient.elasticsearch;

import cn.hutool.core.util.StrUtil;
import com.example.ckb.esclient.client.AliasClient;
import com.example.ckb.esclient.client.DocumentClient;
import com.example.ckb.esclient.client.IndexClient;
import com.example.ckb.esclient.client.SearchClient;
import com.example.ckb.esclient.requests.search.Delete;
import com.example.ckb.esclient.requests.search.Scroll;
import com.example.ckb.esclient.requests.search.Search;
import com.example.ckb.esclient.requests.search.SearchParams;
import com.example.ckb.esclient.response.NodeInfo;
import com.example.ckb.esclient.response.search.SearchResponse;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.WebClientBuilder;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.client.logging.LoggingClient;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.auth.AuthToken;
import com.linecorp.armeria.common.util.Exceptions;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Accessors(fluent = true)
public final class ElasticSearch implements Closeable {
    private final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Getter
    private final WebClient client;
    @Getter
    private final CompletableFuture<ElasticSearchVersion> version;

    private final EndpointGroup endpointGroup;
    private final ClientFactory clientFactory;

    private final IndexClient indexClient;
    private final DocumentClient documentClient;
    private final AliasClient aliasClient;
    private final SearchClient searchClient;

    ElasticSearch(SessionProtocol protocol,
                  String username, String password,
                  EndpointGroup endpointGroup,
                  ClientFactory clientFactory,
                  Duration responseTimeout) {
        this.endpointGroup = endpointGroup;
        this.clientFactory = clientFactory;

        final WebClientBuilder builder =
                WebClient.builder(protocol, endpointGroup)
                        .factory(clientFactory)
                        .responseTimeout(responseTimeout)
                        .decorator(LoggingClient.builder()
                                .logger(log)
                                .newDecorator())
                        .decorator(RetryingClient.builder(RetryRule.failsafe())
                                .maxTotalAttempts(3)
                                .newDecorator());
        if (StrUtil.isNotBlank(username) && StrUtil.isNotBlank(password)) {
            builder.auth(AuthToken.ofBasic(username, password));
        }
        client = builder.build();
        version = new CompletableFuture<>();

        documentClient = new DocumentClient(version, client);
        indexClient = new IndexClient(version, client);
        aliasClient = new AliasClient(version, client);
        searchClient = new SearchClient(version, client);
    }

    public CompletableFuture<ElasticSearchVersion> connect() {
        final CompletableFuture<ElasticSearchVersion> future =
                client.get("/").aggregate().thenApply(response -> {
                    final HttpStatus status = response.status();
                    if (status != HttpStatus.OK) {
                        throw new RuntimeException(
                                "Failed to connect to ElasticSearch server: " + response.contentUtf8());
                    }
                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        final NodeInfo node = mapper.readValue(is, NodeInfo.class);
                        final String vn = node.getVersion().getNumber();
                        final String distribution = node.getVersion().getDistribution();
                        return ElasticSearchVersion.of(distribution, vn);
                    } catch (IOException e) {
                        return Exceptions.throwUnsafely(e);
                    }
                });
        future.whenComplete((v, throwable) -> {
            if (throwable != null) {
                final RuntimeException cause =
                        new RuntimeException("Failed to determine ElasticSearch version", throwable);
                version.completeExceptionally(cause);
                return;
            }
            log.info("ElasticSearch version is: {}", v);
            version.complete(v);
        });
        return future;
    }

    public DocumentClient documents() {
        return documentClient;
    }

    public IndexClient index() {
        return indexClient;
    }

    public AliasClient alias() {
        return aliasClient;
    }

    public SearchResponse search(Search search, SearchParams params, String... index) {
        return searchClient.search(search, params, index);
    }

    public SearchResponse delete(Delete delete, SearchParams params, String... index) {
        return searchClient.delete(delete, params, index);
    }

    public SearchResponse search(Search search, String... index) {
        return search(search, null, index);
    }

    public SearchResponse scroll(Duration contextRetention, String scrollId) {
        return searchClient.scroll(
                Scroll.builder()
                        .contextRetention(contextRetention)
                        .scrollId(scrollId)
                        .build());
    }

    @Override
    public void close() {
        clientFactory.close();
        endpointGroup.close();
    }
}
