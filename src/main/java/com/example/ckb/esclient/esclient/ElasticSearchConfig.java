package com.example.ckb.esclient.esclient;

import com.example.ckb.esclient.elasticsearch.ElasticSearchBuilder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ElasticSearchConfig {

    private String clusterNodes;

    private String protocol = "http";

    private String user;

    private String password;

    private int connectTimeout = 30;

    private int socketTimeout = 30;

    private int responseTimeout = 120;

    private int healthCheckRetryInterval = 30;

    private int numHttpClientThread = 2;

    public ElasticSearchBuilder elasticSearchBuilder() {
        return new ElasticSearchBuilder().clusterNodes(clusterNodes).protocol(protocol).username(user).password(password)
                .connectTimeout(connectTimeout).socketTimeout(socketTimeout).responseTimeout(responseTimeout)
                .healthCheckRetryInterval(healthCheckRetryInterval).numHttpClientThread(numHttpClientThread);
    }
}
