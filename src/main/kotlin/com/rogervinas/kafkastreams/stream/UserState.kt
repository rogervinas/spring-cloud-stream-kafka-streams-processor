package com.rogervinas.kafkastreams.stream

data class UserState(val userId: String = "", val tokens: List<Int> = emptyList(), private val expired: Boolean = false) {

  fun isCompleted(): Boolean {
    return tokens.containsAll(listOf(1, 2, 3, 4, 5))
  }

  fun isExpired(): Boolean {
    return expired
  }

  fun expire() = UserState(userId, tokens, true)

  operator fun plus(event: UserTokenEvent) = UserState(event.userId, tokens + event.token, expired)
}
