package com.solrup.avro

import com.solrup.avro.producer.KafkaProducer

object ProducerApp extends App {

  private val topic = "avro-topic"

  val producer = new KafkaProducer()

  val user1 = User(1, "Steve Cooney", None)
  val user2 = User(2, "Bill Tsay", Some("bill.tsay@solrup.com"))

  producer.send(topic, List(user1, user2))

}