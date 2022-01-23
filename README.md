[![CI](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-processor/actions/workflows/gradle.yml/badge.svg?branch=master)](https://github.com/rogervinas/spring-cloud-stream-kafka-streams-processor/actions/workflows/gradle.yml)

# Spring Cloud Stream & Kafka Streams Processor

* [Test this demo](#test-this-demo)
* [Run this demo](#run-this-demo)
* [See also](#see-also)

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
