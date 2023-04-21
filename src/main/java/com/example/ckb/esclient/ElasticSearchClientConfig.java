package com.example.ckb.esclient;

import com.example.ckb.config.StorageESConfig;
import com.example.ckb.esclient.esclient.ElasticSearchClient;
import com.example.ckb.esclient.esclient.ElasticSearchConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;


@Slf4j
@Configuration
public class ElasticSearchClientConfig {

    @Autowired
    private StorageESConfig storageESConfig;


    @Bean("ElasticSearchClient")
    @Primary
    public ElasticSearchClient getHighLevelClient() {
        try {
        ElasticSearchConfig config = new ElasticSearchConfig();
        config.setProtocol(storageESConfig.getProtocol());
        config.setNumHttpClientThread(storageESConfig.getNumHttpClientThread());
        config.setSocketTimeout(storageESConfig.getSocketTimeout());
        config.setConnectTimeout(storageESConfig.getConnectTimeout());
        config.setClusterNodes(storageESConfig.getClusterNodes());
        return new ElasticSearchClient(config);
        } catch (Exception e) {
            log.error("ElasticSearchClient 初始化失败", e);
        }
        return null;

    }



}
