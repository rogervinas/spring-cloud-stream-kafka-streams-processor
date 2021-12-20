package com.rogervinas.kafkastreams.application

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MyApplication

fun main(args: Array<String>) {
  runApplication<MyApplication>(*args)
}
