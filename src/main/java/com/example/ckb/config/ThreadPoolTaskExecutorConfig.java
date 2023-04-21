package com.example.ckb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ThreadPoolTaskExecutorConfig {

    private int corePoolSize;
    private int maxPoolSize;
    private int queueCapacity;
    private int keepAliveSeconds;


    @Bean(name = "executorService")
    public ExecutorService executorService() {
        ExecutorService executor = new ThreadPoolExecutor(10,10,0, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());

//        ThreadPoolExecutor executor = new ThreadPoolExecutor(5,10,5, TimeUnit.SECONDS,new LinkedBlockingQueue<>());

        return executor;
    }
}
