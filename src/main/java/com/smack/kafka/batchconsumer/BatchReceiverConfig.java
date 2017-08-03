package com.smack.kafka.batchconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchReceiverConfig {

    public static final int NUMBER_OF_MESSAGES = 20;
    public static final int NUMBER_OF_RECORDS_IN_SINGLE_BATCH = 10;

    private static final String CONSUMER_GROUP_ID = "batch";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public Map<String, Object> batchConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        // max number of records in a single batch poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, NUMBER_OF_RECORDS_IN_SINGLE_BATCH);

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> batchConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(batchConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory());

        // enable batch listener
        factory.setBatchListener(true);

        return factory;
    }

    @Bean
    public BatchReceiver batchReceiver() {
        return new BatchReceiver();
    }
}
