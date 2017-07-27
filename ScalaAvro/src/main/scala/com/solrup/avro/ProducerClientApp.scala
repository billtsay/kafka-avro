package com.solrup.avro

import com.solrup.avro.clients.Producer

object ProducerClientApp extends App {

  private val topic = "avro-topic"

  val producer = new Producer()

  val user1 = User(1, "Steve Cooney", None)
  val user2 = User(2, "Bill Tsay", Some("bill.tsay@solrup.com"))

  producer.send(topic, List(user1, user2))

}