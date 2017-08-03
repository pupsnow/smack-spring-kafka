package com.smack.kafka.stringconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;


public class StringReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringReceiver.class);

    private CountDownLatch consumerLatch = new CountDownLatch(1);

    public CountDownLatch getConsumerLatch() {
        return consumerLatch;
    }

    @KafkaListener(topics = "${topic.string}", containerFactory = "stringKafkaListenerContainerFactory")
    public void receive(String strMessage) {
        LOGGER.info("received string message = '{}'", strMessage);
        consumerLatch.countDown();
    }

}
