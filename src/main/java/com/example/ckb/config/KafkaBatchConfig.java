package com.example.ckb.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaBatchConfig {

    //    @Value("${kafka.boostrap.servers}")
    private String bootstrapServers = "192.168.15.147:9092";

    @Bean
    KafkaListenerContainerFactory batchFactory() {

        ConcurrentKafkaListenerContainerFactory factory = new

                ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerConfigs()));

        factory.setBatchListener(true); // 开启批量监听

        return factory;

    }

    @Bean
    public Map consumerConfigs() {

        Map props = new HashMap<>();

//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); //设置每次接收Message的数量

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5");

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 120000);

        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 180000);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;

    }
}
