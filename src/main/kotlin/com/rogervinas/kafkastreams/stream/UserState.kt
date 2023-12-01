package com.rogervinas.kafkastreams.stream

data class UserState(val userId: String = "", val tokens: List<Int> = emptyList(), val expired: Boolean = false) {

  val completed = tokens.containsAll(listOf(1, 2, 3, 4, 5))

  fun expire() = UserState(userId, tokens, true)

  operator fun plus(event: UserTokenEvent) = UserState(event.userId, tokens + event.token, expired)
}
