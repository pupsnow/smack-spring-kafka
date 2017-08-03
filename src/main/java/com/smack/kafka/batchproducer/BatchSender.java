package com.smack.kafka.batchproducer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;


public class BatchSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchSender.class);

    @Autowired
    private KafkaTemplate<String, String> batchKafkaTemplate;

    public void send(String topic, String data) {

        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        batchKafkaTemplate.send(topic, data);

    }
}
