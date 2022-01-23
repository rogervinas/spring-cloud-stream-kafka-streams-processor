[![CI](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-processor/actions/workflows/gradle.yml/badge.svg?branch=master)](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-processor/actions/workflows/gradle.yml)

# Spring Cloud Stream & Kafka Streams Binder + Processor API

* [Test-first using kafka-streams-test-utils](#test-first-using-kafka-streams-test-utils)
* [Test this demo](#test-this-demo)
* [Run this demo](#run-this-demo)
* [See also](#see-also)

[Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream) is the solution provided by **Spring** to build applications connected to shared messaging systems.

It offers an abstraction (the **binding**) that works the same whatever underneath implementation we use (the **binder**):
* **Apache Kafka**
* **Rabbit MQ**
* **Kafka Streams**
* **Amazon Kinesis**
* ...

In my previous post [Spring Cloud Stream Kafka Streams first steps](https://dev.to/adevintaspain/spring-cloud-stream-kafka-stream-binder-first-steps-1pch) I got working a simple example using the **Kafka Streams binder**.

In this one the goal is to use the **Kafka Streams binder** and the  [Kafka Streams Processor API](https://kafka.apache.org/10/documentation/streams/developer-guide/processor-api.html) to implement the following scenario:

![Diagram](doc/diagram.png)

1. We receive messages with key = **userId** and value = { userId: string, token: number } from topic **pub.user.token**

2. For every **userId** which we receive **token** 1, 2, 3, 4 and 5 within under 1 minute, we send a **completed** event to topic **pub.user.state**

3. For every **userId** which we receive at least one **token** but not the complete 1, 2, 3, 4 and 5 sequence within under 1 minute, we send an **expired** event to topic **pub.user.state**

Ready? Let's code! 🤓

## Test-first using kafka-streams-test-utils

Once kafka-streams-test-utils is properly setup in our [@BeforeEach](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-processor/blob/master/src/test/kotlin/com/rogervinas/kafkastreams/stream/UserStreamTest.kt#L39) ...

```kotlin
data class UserTokenEvent(val userId: String, val token: Int)

enum class UserStateEventType { COMPLETED, EXPIRED }
data class UserStateEvent(val userId: String, val state: UserStateEventType)

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
```

## UserStateStream implementation

We start first with Our UserStateStream as a **Function**:
* Which input is a **KStream<String, UserTokenEvent>**, as we want a **String** as the Kafka message's key and a **UserTokenEvent** as the Kafka message's value
* Which output is a **KStream<String, UserStateEvent>**, same here, **String** as the key and **UserStateEvent** as the value 

```kotlin
class UserStateStream(
  private val schedule: Duration,
  private val expiration: Duration
) : Function<KStream<String, UserTokenEvent>, KStream<String, UserStateEvent>> {

  override fun apply(input: KStream<String, UserTokenEvent>): KStream<String, UserStateEvent> {
    TODO()
  }
}
```

Now step by step ...

### 1. Aggregation by userId

```kotlin
private const val USER_STATE_STORE = "user-state"

data class UserState(val userId: String = "", val tokens: List<Int> = emptyList()) {
  operator fun plus(event: UserTokenEvent) = UserState(event.userId, tokens + event.token)
}

class UserStateStream(
  private val schedule: Duration,
  private val expiration: Duration
) : Function<KStream<String, UserTokenEvent>, KStream<String, UserStateEvent>> {
  override fun apply(input: KStream<String, UserTokenEvent>): KStream<String, UserStateEvent> {
    return input
      .selectKey { _, event -> event.userId } // just in case but the key should be userId already
      .groupByKey()
      .aggregate(
        { UserState() },
        { userId, event, state ->
          logger.info("Aggregate $userId ${state.tokens} + ${event.token}")
          state + event // we use the UserState's plus operator
        },
        Materialized.`as`<String, UserState, KeyValueStore<Bytes, ByteArray>>(USER_STATE_STORE)
          .withKeySerde(Serdes.StringSerde())
          .withValueSerde(JsonSerde(UserState::class.java))
      )
      .toStream()
      // From here down it is just to avoid compilation errors
      .mapValues { userId, _ ->
        UserStateEvent(userId, COMPLETED) 
      }
  }
}
```

## Test this demo

```shell
./gradlew test
```

## Run this demo

Run with docker-compose:
```shell
docker-compose up -d
./gradlew bootRun
docker-compose down
```

Then you can use [kcat](https://github.com/edenhill/kcat) (formerly know as **kafkacat**) to produce/consume to/from **Kafka**:
```shell
# consume
kcat -b localhost:9094 -C -t pub.user.token -f '%k %s\n'
kcat -b localhost:9094 -C -t pub.user.state -f '%k %s\n'

# produce
echo '1:{"userId":"1", "token":1}' | kcat -b localhost:9094 -P -t pub.user.token -K:
echo '1:{"userId":"1", "token":2}' | kcat -b localhost:9094 -P -t pub.user.token -K:
echo '1:{"userId":"1", "token":3}' | kcat -b localhost:9094 -P -t pub.user.token -K:
echo '1:{"userId":"1", "token":4}' | kcat -b localhost:9094 -P -t pub.user.token -K:
echo '1:{"userId":"1", "token":5}' | kcat -b localhost:9094 -P -t pub.user.token -K:
```

## See also

:octocat: [Spring Cloud Stream Kafka step by step](https://github.com/rogervinas/spring-cloud-stream-kafka-step-by-step)

:octocat: [Spring Cloud Stream & Kafka Streams Binder first steps](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-first-steps)

:octocat: [Spring Cloud Stream Multibinder](https://github.com/rogervinas/spring-cloud-stream-multibinder)
