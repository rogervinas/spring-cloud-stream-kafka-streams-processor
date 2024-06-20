package com.rogervinas.kafkastreams.application

import com.rogervinas.kafkastreams.stream.UserStateEvent
import com.rogervinas.kafkastreams.stream.UserStateStream
import com.rogervinas.kafkastreams.stream.UserTokenEvent
import org.apache.kafka.streams.kstream.KStream
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration
import java.util.function.Function

@Configuration
class ApplicationConfiguration {
  @Bean
  fun userStateStream(
    @Value("\${user.schedule}") schedule: Duration,
    @Value("\${user.expiration}") expiration: Duration,
  ): Function<KStream<String, UserTokenEvent>, KStream<String, UserStateEvent>> = UserStateStream(schedule, expiration)
}
