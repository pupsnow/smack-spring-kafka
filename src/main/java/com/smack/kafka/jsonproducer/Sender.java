package com.smack.kafka.jsonproducer;

import com.smack.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class Sender {

  private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

  @Value("${topic.json}")
  private String jsonTopic;

  @Autowired
  private KafkaTemplate<String, Message> kafkaTemplate;

  public void send(Message msg) {
    LOGGER.info("sending msg = '{}'", msg.toString());

    ListenableFuture<SendResult<String, Message>> future = kafkaTemplate.send(jsonTopic, msg);

    future.addCallback(new ListenableFutureCallback<SendResult<String, Message>>() {

      @Override
      public void onSuccess(SendResult<String, Message> result) {
        LOGGER.info("sent message='{}' with offset={}", msg,
                result.getRecordMetadata().offset());
      }

      @Override
      public void onFailure(Throwable ex) {
        LOGGER.error("unable to sendString message='{}'", msg, ex);
      }
    });
  }
}
