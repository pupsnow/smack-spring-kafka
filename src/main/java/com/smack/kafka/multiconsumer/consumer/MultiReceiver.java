package com.smack.kafka.multiconsumer.consumer;


import com.smack.kafka.model.Bar;
import com.smack.kafka.model.Foo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class MultiReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiReceiver.class);

    private CountDownLatch latch = new CountDownLatch(10);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${topic.bar}", containerFactory = "multiConsumerKafkaListenerContainerFactory")
    public void receiveBarMessage(Bar bar) {
        LOGGER.info("received {}", bar.toString());
        latch.countDown();
    }

    @KafkaListener(topics = "${topic.foo}", containerFactory = "multiConsumerKafkaListenerContainerFactory")
    public void receiveFooMessage(Foo foo) {
        LOGGER.info("received {}", foo.toString());
        latch.countDown();
    }
}
