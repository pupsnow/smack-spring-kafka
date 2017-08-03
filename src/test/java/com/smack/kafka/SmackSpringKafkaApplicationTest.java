package com.smack.kafka;

import java.util.concurrent.TimeUnit;

import com.smack.kafka.batchconsumer.BatchReceiver;
import com.smack.kafka.batchconsumer.BatchReceiverConfig;
import com.smack.kafka.batchproducer.BatchSender;
import com.smack.kafka.jsonconsumer.Receiver;
import com.smack.kafka.jsonproducer.Sender;
import com.smack.kafka.model.Message;
import com.smack.kafka.stringconsumer.StringReceiver;
import com.smack.kafka.stringproducer.StringSender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(SpringRunner.class)
@SpringBootTest
//@TestPropertySource(properties = "debug=true")
public class SmackSpringKafkaApplicationTest {

  private static final String BATCH_TOPIC_NAME = "batch.t";
  private static final String JSON_MSG_TOPIC_NAME = "json.t";
  private static final String STRING_MSG_TOPIC_NAME = "string.t";


  @Autowired
  private Sender jsonSender;

  @Autowired
  private Receiver jsonReceiver;

  @Autowired
  private StringSender stringSender;

  @Autowired
  private StringReceiver stringReceiver;

  @Autowired
  private BatchSender batchSender;

  @Autowired
  private BatchReceiver batchReceiver;

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @ClassRule
  public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, JSON_MSG_TOPIC_NAME, STRING_MSG_TOPIC_NAME, BATCH_TOPIC_NAME);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty("kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
  }

  @Before
  public void setUp() throws Exception {
    // waiting for partitions to be assigned
    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
        .getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer,
          embeddedKafka.getPartitionsPerTopic());
    }
  }

  @Test
  public void testReceive() throws Exception {
    //given
    Message msg = new Message("ID1", "120/60", "some description");
    //when
    jsonSender.send(msg);
    jsonReceiver.getConsumerLatch().await(10000, TimeUnit.MILLISECONDS);
    //then
    assertThat(jsonReceiver.getConsumerLatch().getCount()).isEqualTo(0);

  }

  @Test
  public void testStringReceive() throws Exception {
    //given
    //when
    stringSender.send("string message");
    stringReceiver.getConsumerLatch().await(10000, TimeUnit.MILLISECONDS);
    //then
    assertThat(stringReceiver.getConsumerLatch().getCount()).isEqualTo(0);
  }

  @Test
  public void testBatchSendReceive() throws Exception {
    //given
    int numberOfMessages = BatchReceiverConfig.NUMBER_OF_MESSAGES;
    //when
    for (int i = 0; i < numberOfMessages; i++) {
      batchSender.send(BATCH_TOPIC_NAME, "message " + i);
    }

    //then
    batchReceiver.getConsumerLatch().await(10000, TimeUnit.MILLISECONDS);
    assertThat(batchReceiver.getConsumerLatch().getCount()).isEqualTo(0);
  }
}
