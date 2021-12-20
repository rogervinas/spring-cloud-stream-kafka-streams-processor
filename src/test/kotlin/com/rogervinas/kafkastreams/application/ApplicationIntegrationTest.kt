package com.rogervinas.kafkastreams.application

import com.rogervinas.kafkastreams.helper.DockerComposeContainerHelper
import com.rogervinas.kafkastreams.helper.KafkaConsumerHelper
import com.rogervinas.kafkastreams.helper.KafkaProducerHelper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.function.Consumer

private const val TOPIC_USER_TOKEN = "pub.user.token"
private const val TOPIC_USER_STATE = "pub.user.state"

private const val USERNAME_1 = "user1"

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class ApplicationIntegrationTest {

  companion object {

    @Container
    val container = DockerComposeContainerHelper().createContainer()
  }

  @Value("\${spring.cloud.stream.kafka.streams.binder.brokers}")
  lateinit var kafkaBroker: String
  lateinit var kafkaProducerHelper: KafkaProducerHelper
  lateinit var kafkaConsumerHelper: KafkaConsumerHelper

  @BeforeEach
  fun beforeEach() {
    kafkaProducerHelper = KafkaProducerHelper(kafkaBroker)
    kafkaConsumerHelper = KafkaConsumerHelper(kafkaBroker, TOPIC_USER_STATE)
    kafkaConsumerHelper.consumeAll()
  }

  @Test
  fun `should publish completed event`() {
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 1}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 2}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 3}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 4}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 5}""")

    val records = kafkaConsumerHelper.consumeAtLeast(1, Duration.ofMinutes(1))

    assertThat(records).singleElement().satisfies(Consumer {
      assertThat(it.key()).isEqualTo(USERNAME_1)
      JSONAssert.assertEquals("""{"userId": "$USERNAME_1", "state": "completed"}""", it.value(), true)
    })
  }

  @Test
  fun `should publish expired event`() {
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 1}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 2}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 3}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, USERNAME_1, """{"userId": "$USERNAME_1", "token": 4}""")

    val records = kafkaConsumerHelper.consumeAtLeast(1, Duration.ofMinutes(1))

    assertThat(records).singleElement().satisfies(Consumer {
      assertThat(it.key()).isEqualTo(USERNAME_1)
      JSONAssert.assertEquals("""{"userId": "$USERNAME_1", "state": "expired"}""", it.value(), true)
    })
  }
}
