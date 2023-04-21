package com.example.ckb.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Data
public class StorageESConfig {

    @Value("${storage.elasticsearch.hostclusterNodes}")
    private String clusterNodes;
    @Value("${storage.elasticsearch.protocol:http}")
    private String protocol;
    @Value("${storage.elasticsearch.connectTimeout:5000}")
    private int connectTimeout;
    @Value("${storage.elasticsearch.socketTimeout:30000}")
    private int socketTimeout;
    @Value("${storage.elasticsearch.numHttpClientThread:200}")
    private int numHttpClientThread;

}
