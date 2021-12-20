package com.rogervinas.kafkastreams.stream

enum class UserStateEventType {
  COMPLETED,
  EXPIRED
}

data class UserStateEvent(val userId: String, val state: UserStateEventType)
