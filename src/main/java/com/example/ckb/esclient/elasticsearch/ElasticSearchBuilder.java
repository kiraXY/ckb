package com.example.ckb.esclient.elasticsearch;

import cn.hutool.core.util.StrUtil;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.EndpointGroup;
import com.linecorp.armeria.client.endpoint.healthcheck.HealthCheckedEndpointGroup;
import com.linecorp.armeria.client.endpoint.healthcheck.HealthCheckedEndpointGroupBuilder;
import com.linecorp.armeria.client.logging.LoggingClient;
import com.linecorp.armeria.client.retry.Backoff;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.auth.AuthToken;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Slf4j
public final class ElasticSearchBuilder {
    private static final int NUM_PROC = Runtime.getRuntime().availableProcessors();

    private SessionProtocol protocol = SessionProtocol.HTTP;

    private String username;

    private String password;

    private Duration healthCheckRetryInterval;

    private String clusterNodes;

    private Duration responseTimeout;

    private Duration connectTimeout;

    private Duration socketTimeout;

    private int numHttpClientThread;

    public ElasticSearchBuilder protocol(String protocol) {
        if (StrUtil.isBlank(protocol)) {
            return this;
        }
        this.protocol = SessionProtocol.of(protocol);
        return this;
    }

    public ElasticSearchBuilder clusterNodes(String clusterNodes) {
        this.clusterNodes = requireNonNull(clusterNodes, "clusterNodes");;
        return this;
    }

    public ElasticSearchBuilder username(String username) {
        if (StrUtil.isBlank(username)) {
            return this;
        }
        this.username = username;
        return this;
    }

    public ElasticSearchBuilder password(String password) {
        if (StrUtil.isBlank(password)) {
            return this;
        }
        this.password = password;
        return this;
    }

    public ElasticSearchBuilder healthCheckRetryInterval(int healthCheckRetryInterval) {
        this.healthCheckRetryInterval = Duration.ofSeconds(healthCheckRetryInterval);
        return this;
    }

    public ElasticSearchBuilder connectTimeout(int connectTimeout) {
        this.connectTimeout = Duration.ofSeconds(connectTimeout);
        return this;
    }

    public ElasticSearchBuilder responseTimeout(int responseTimeout) {
        this.responseTimeout = Duration.ofSeconds(responseTimeout);
        return this;
    }

    public ElasticSearchBuilder socketTimeout(int socketTimeout) {
        this.socketTimeout = Duration.ofSeconds(socketTimeout);
        return this;
    }

    public ElasticSearchBuilder numHttpClientThread(int numHttpClientThread) {
        this.numHttpClientThread = numHttpClientThread;
        return this;
    }

    @SneakyThrows
    public ElasticSearch build() {
        final List<Endpoint> endpoints =
                Arrays.stream(this.clusterNodes.split(","))
                        .filter(StrUtil::isNotBlank)
                        .map(Endpoint::parse)
                        .collect(Collectors.toList());
        final ClientFactoryBuilder factoryBuilder =
                ClientFactory.builder()
                        .connectTimeout(connectTimeout)
                        .idleTimeout(socketTimeout)
                        .useHttp2Preface(false)
                        .workerGroup(numHttpClientThread > 0 ? numHttpClientThread : NUM_PROC);

        final ClientFactory clientFactory = factoryBuilder.build();

        final HealthCheckedEndpointGroupBuilder endpointGroupBuilder =
                HealthCheckedEndpointGroup.builder(EndpointGroup.of(endpoints), "_cluster/health")
                        .protocol(protocol)
                        .useGet(true)
                        .clientFactory(clientFactory)
                        .retryBackoff(Backoff.fibonacci(10, 100))
                        .retryInterval(healthCheckRetryInterval)
                        .withClientOptions(options -> {
                            options.decorator(
                                    LoggingClient.builder()
                                            .logger(log)
                                            .newDecorator());
                            options.decorator((delegate, ctx, req) -> {
                                ctx.logBuilder().name("health-check");
                                return delegate.execute(ctx, req);
                            });
                            return options;
                        });
        if (StrUtil.isNotBlank(username) && StrUtil.isNotBlank(password)) {
            endpointGroupBuilder.auth(AuthToken.ofBasic(username, password));
        }
        final HealthCheckedEndpointGroup endpointGroup = endpointGroupBuilder.build();

        return new ElasticSearch(
                protocol,
                username,
                password,
                endpointGroup,
                clientFactory,
                responseTimeout
        );
    }
}
