package com.rogervinas.kafkastreams.stream

import com.rogervinas.kafkastreams.stream.UserStateEventType.COMPLETED
import com.rogervinas.kafkastreams.stream.UserStateEventType.EXPIRED
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.serializer.JsonSerde
import java.time.Duration
import java.util.function.Function

private const val USER_STATE_STORE = "user-state"

class UserStateStream(
  private val schedule: Duration,
  private val expiration: Duration,
) : Function<KStream<String, UserTokenEvent>, KStream<String, UserStateEvent>> {
  private val logger = LoggerFactory.getLogger(UserStateStream::class.java)

  override fun apply(input: KStream<String, UserTokenEvent>): KStream<String, UserStateEvent> {
    return input
      .selectKey { _, event -> event.userId }
      .groupByKey()
      .aggregate(
        { UserState() },
        { userId, event, state ->
          logger.info("Aggregate $userId ${state.tokens} + ${event.token}")
          state + event
        },
        Materialized.`as`<String, UserState, KeyValueStore<Bytes, ByteArray>>(USER_STATE_STORE)
          .withKeySerde(Serdes.StringSerde())
          .withValueSerde(JsonSerde(UserState::class.java)),
      )
      .toStream()
      .apply { process(ProcessorSupplier { UserStateProcessor(schedule, expiration) }, USER_STATE_STORE) }
      .mapValues { state ->
        logger.info("State $state")
        when {
          state == null -> null
          state.completed -> UserStateEvent(state.userId, COMPLETED)
          state.expired -> UserStateEvent(state.userId, EXPIRED)
          else -> null
        }
      }
      .filter { _, event -> event != null }
      .mapValues { event ->
        logger.info("Publish $event")
        event!!
      }
  }
}

class UserStateProcessor(
  private val schedule: Duration,
  private val expiration: Duration,
) : Processor<String, UserState, Void, Void> {
  private val logger = LoggerFactory.getLogger(UserStateProcessor::class.java)

  override fun init(context: ProcessorContext<Void, Void>) {
    context.schedule(schedule, PunctuationType.WALL_CLOCK_TIME) { time ->
      val stateStore = context.getStateStore<KeyValueStore<String, ValueAndTimestamp<UserState>>>(USER_STATE_STORE)
      stateStore.all().forEachRemaining {
        val age = Duration.ofMillis(time - it.value.timestamp())
        if (age > expiration) {
          if (it.value.value().expired) {
            logger.info("Delete ${it.key}")
            stateStore.delete(it.key)
          } else {
            logger.info("Expire ${it.key}")
            stateStore.put(it.key, ValueAndTimestamp.make(it.value.value().expire(), it.value.timestamp()))
          }
        }
      }
    }
  }

  override fun process(record: Record<String, UserState>?) {
  }
}
