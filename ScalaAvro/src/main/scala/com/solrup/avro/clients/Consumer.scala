package com.solrup.avro.clients

import java.util.{ Properties, Collections }
import java.util.concurrent._

import com.solrup.avro.User
import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.consumer.{ KafkaConsumer, ConsumerConfig }
import kafka.serializer.DefaultDecoder

import scala.io.Source

class Consumer() {
  private val props = new Properties()

  val groupId = "avro-topic-consumer"
  val topic = "avro-topic"

  props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  private val consumer = new KafkaConsumer[String, Array[Byte]](props)
  consumer.subscribe(Collections.singletonList(this.topic))

  lazy val iterator = consumer.poll(1000).iterator()

  def shutdown() = {
    if (consumer != null)
      consumer.close();

  }

  val schemaString = Source.fromURL(getClass.getResource("/schema.avsc")).mkString
  // Initialize schema
  val schema: Schema = new Schema.Parser().parse(schemaString)

  private def getUser(message: Array[Byte]): Option[User] = {

    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val userData: GenericRecord = reader.read(null, decoder)

    // Make user object
    val user = User(userData.get("id").toString.toInt, userData.get("name").toString, try {
      Some(userData.get("email").toString)
    } catch {
      case _ => None
    })
    Some(user)
  }

  /**
   * Read message from kafka queue
   *
   * @return Some of message if exist in kafka queue, otherwise None
   */
  def read() =
    try {
      if (hasNext) {
        println("Getting message from queue.............")
        val message: Array[Byte] = iterator.next().value()
        getUser(message)
      } else {
        None
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        None
    }

  private def hasNext: Boolean =
    try
      iterator.hasNext()
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        println("Got error when reading message ")
        false
    }

}