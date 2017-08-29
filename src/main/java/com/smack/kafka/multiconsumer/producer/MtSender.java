package com.smack.kafka.multiconsumer.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class MtSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(MtSender.class);

    @Autowired
    private KafkaTemplate<String, ?> mtProducerKafkaTemplate;


    public void send(Message<?> record) {
        LOGGER.info("Sending {}", record.toString());
        ListenableFuture<? extends SendResult<String, ?>> future = mtProducerKafkaTemplate.send(record);

        future.addCallback(new ListenableFutureCallback<SendResult<String, ?>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("Unable to send message {}", record, ex);
            }

            @Override
            public void onSuccess(SendResult<String, ?> result) {
                LOGGER.info("Sent message sender info {}", result.getProducerRecord());
                LOGGER.info("Sent message broker info {}", result.getRecordMetadata());
            }
        });

    }



}
