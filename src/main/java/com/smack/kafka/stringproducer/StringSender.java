package com.smack.kafka.stringproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;


public class StringSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringSender.class);

    @Value("${topic.string}")
    private String stringTopic;

    @Autowired
    private KafkaTemplate<String, String> stringKafkaTemplate;

    public void send(String str) {
        LOGGER.info("sending str message = '{}'", str);
        stringKafkaTemplate.send(stringTopic, str);
    }
}
