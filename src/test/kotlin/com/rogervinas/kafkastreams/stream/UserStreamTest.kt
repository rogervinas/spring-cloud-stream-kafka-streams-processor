package com.rogervinas.kafkastreams.stream

import com.rogervinas.kafkastreams.stream.UserStateEventType.COMPLETED
import com.rogervinas.kafkastreams.stream.UserStateEventType.EXPIRED
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerde
import java.time.Duration
import java.util.Properties
import java.util.function.Consumer

private const val TOPIC_IN = "topic.in"
private const val TOPIC_OUT = "topic.out"

private const val USERNAME_1 = "user1"
private const val USERNAME_2 = "user2"
private const val USERNAME_3 = "user3"
private const val USERNAME_4 = "user4"

private val SCHEDULE = Duration.ofSeconds(1)
private val EXPIRATION = Duration.ofSeconds(5)

class UserStreamTest {

  private lateinit var topologyTestDriver: TopologyTestDriver
  private lateinit var topicIn: TestInputTopic<String, UserTokenEvent>
  private lateinit var topicOut: TestOutputTopic<String, UserStateEvent>

  @BeforeEach
  fun beforeEach() {
    val stringSerde = Serdes.StringSerde()
    val streamsBuilder = StreamsBuilder()

    UserStateStream(SCHEDULE, EXPIRATION)
      .apply(streamsBuilder.stream(TOPIC_IN))
      .to(TOPIC_OUT)

    val config = Properties().apply {
      setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.javaClass.name)
      setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde::class.java.name)
      setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + System.currentTimeMillis())
      setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test-server")
      setProperty(JsonDeserializer.TRUSTED_PACKAGES, "*")
    }
    val topology = streamsBuilder.build()
    topologyTestDriver = TopologyTestDriver(topology, config)
    topicIn = topologyTestDriver.createInputTopic(
      TOPIC_IN,
      stringSerde.serializer(),
      JsonSerde(UserTokenEvent::class.java).serializer()
    )
    topicOut = topologyTestDriver.createOutputTopic(
      TOPIC_OUT,
      stringSerde.deserializer(),
      JsonSerde(UserStateEvent::class.java).deserializer()
    )
  }

  @AfterEach
  fun afterEach() {
    topologyTestDriver.close()
  }

  @Test
  fun `should publish completed event for one user`() {
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 1))
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 2))
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 3))
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 4))
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 5))

    topologyTestDriver.advanceWallClockTime(EXPIRATION.minusMillis(10))

    assertThat(topicOut.readKeyValuesToList()).singleElement().satisfies(Consumer { topicOutMessage ->
      assertThat(topicOutMessage.key).isEqualTo(USERNAME_1)
      assertThat(topicOutMessage.value).isEqualTo(UserStateEvent(USERNAME_1, COMPLETED))
    })
  }

  @Test
  fun `should publish expired event for one user`() {
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 1))
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 2))

    topologyTestDriver.advanceWallClockTime(EXPIRATION.plus(SCHEDULE).plus(SCHEDULE))

    assertThat(topicOut.readKeyValuesToList()).singleElement().satisfies(Consumer { topicOutMessage ->
      assertThat(topicOutMessage.key).isEqualTo(USERNAME_1)
      assertThat(topicOutMessage.value).isEqualTo(UserStateEvent(USERNAME_1, EXPIRED))
    })
  }

  @Test
  fun `should generate events for many users`() {
    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 1))
    topicIn.pipeInput(USERNAME_2, UserTokenEvent(USERNAME_2, 1))
    topicIn.pipeInput(USERNAME_3, UserTokenEvent(USERNAME_3, 1))
    topicIn.pipeInput(USERNAME_4, UserTokenEvent(USERNAME_4, 1))

    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 2))
    topicIn.pipeInput(USERNAME_2, UserTokenEvent(USERNAME_2, 2))
    topicIn.pipeInput(USERNAME_3, UserTokenEvent(USERNAME_3, 2))
    topicIn.pipeInput(USERNAME_4, UserTokenEvent(USERNAME_4, 2))

    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 3))
    topicIn.pipeInput(USERNAME_2, UserTokenEvent(USERNAME_2, 3))
    topicIn.pipeInput(USERNAME_3, UserTokenEvent(USERNAME_3, 3))
    topicIn.pipeInput(USERNAME_4, UserTokenEvent(USERNAME_4, 3))

    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 4))
    topicIn.pipeInput(USERNAME_2, UserTokenEvent(USERNAME_2, 4))
    topicIn.pipeInput(USERNAME_4, UserTokenEvent(USERNAME_4, 4))

    topicIn.pipeInput(USERNAME_1, UserTokenEvent(USERNAME_1, 5))
    topicIn.pipeInput(USERNAME_4, UserTokenEvent(USERNAME_4, 5))

    topologyTestDriver.advanceWallClockTime(EXPIRATION.plus(SCHEDULE).plus(SCHEDULE))

    assertThat(topicOut.readKeyValuesToMap()).containsExactlyEntriesOf(
      mapOf(
        Pair(USERNAME_1, UserStateEvent(USERNAME_1, COMPLETED)),
        Pair(USERNAME_2, UserStateEvent(USERNAME_2, EXPIRED)),
        Pair(USERNAME_3, UserStateEvent(USERNAME_3, EXPIRED)),
        Pair(USERNAME_4, UserStateEvent(USERNAME_4, COMPLETED))
      )
    )
  }
}
