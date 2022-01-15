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
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import org.testcontainers.shaded.org.awaitility.Durations.*
import java.util.UUID
import java.util.function.Consumer

private const val TOPIC_USER_TOKEN = "pub.user.token"
private const val TOPIC_USER_STATE = "pub.user.state"

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
    val username = UUID.randomUUID().toString()

    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 1}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 2}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 3}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 4}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 5}""")

    await().atMost(ONE_MINUTE).untilAsserted {
      val record = kafkaConsumerHelper.consumeAtLeast(1, ONE_SECOND)
      assertThat(record).singleElement().satisfies(Consumer {
        assertThat(it.key()).isEqualTo(username)
        JSONAssert.assertEquals("""{"userId": "$username", "state": "COMPLETED"}""", it.value(), true)
      })
    }
  }

  @Test
  fun `should publish expired event`() {
    val username = UUID.randomUUID().toString()

    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 1}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 2}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 3}""")
    kafkaProducerHelper.send(TOPIC_USER_TOKEN, username, """{"userId": "$username", "token": 4}""")

    await().atMost(ONE_MINUTE).untilAsserted {
      val record = kafkaConsumerHelper.consumeAtLeast(1, ONE_SECOND)
      assertThat(record).singleElement().satisfies(Consumer {
        assertThat(it.key()).isEqualTo(username)
        JSONAssert.assertEquals("""{"userId": "$username", "state": "EXPIRED"}""", it.value(), true)
      })
    }
  }
}
