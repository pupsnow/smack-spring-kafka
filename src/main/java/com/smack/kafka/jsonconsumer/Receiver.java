package com.smack.kafka.jsonconsumer;

import java.util.concurrent.CountDownLatch;

import com.smack.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

public class Receiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

  private CountDownLatch consumerLatch = new CountDownLatch(1);

  public CountDownLatch getConsumerLatch() {
    return consumerLatch;
  }

  @KafkaListener(topics = "${topic.json}", containerFactory = "kafkaListenerContainerFactory")
  public void receive(Message msg,
                      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                      @Header(KafkaHeaders.OFFSET) Long offset,
                      @Header(KafkaHeaders.RECEIVED_TOPIC) String fromTopic) {
    LOGGER.info("received msg = '{}' | partitionId = {} | offset = {} | fromTopic = {}", msg.toString(), partitionId, offset, fromTopic);
    consumerLatch.countDown();
  }
}
