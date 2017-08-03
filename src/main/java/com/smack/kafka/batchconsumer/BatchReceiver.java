package com.smack.kafka.batchconsumer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class BatchReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchReceiver.class);

    private CountDownLatch consumerLatch = new CountDownLatch(BatchReceiverConfig.NUMBER_OF_MESSAGES);

    public CountDownLatch getConsumerLatch() {
        return consumerLatch;
    }

    @KafkaListener(id = "batch-listener", topics = "${topic.batch}", containerFactory = "batchKafkaListenerContainerFactory")
    public void receive(List<String> data,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET) List<Long> offsets,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics) {

        LOGGER.info("start of batch receive");

        for (int i = 0; i < data.size(); i++) {
            LOGGER.info("received message='{}' with partition-offset='{}' from topic='{}'", data.get(i),
                    partitions.get(i) + "-" + offsets.get(i), topics.get(i));

            consumerLatch.countDown();
        }

        LOGGER.info("end of batch receive");
    }
}
