package com.solrup.avro.clients

import java.util.{ Properties, UUID }
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import com.solrup.avro.User
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.avro.io._
import scala.io.Source
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }

class Producer() {

  private val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("client.id", UUID.randomUUID().toString())

  private val producer = new KafkaProducer[String, Array[Byte]](props)

  //Read avro schema file and
  val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)

  def send(topic: String, users: List[User]): Unit = {
    val genericUser: GenericRecord = new GenericData.Record(schema)
    try {
      val queueMessages = users.map { user =>
        // Create avro generic record object
        //Put data in that generic record object
        genericUser.put("id", user.id)
        genericUser.put("name", user.name)
        genericUser.put("email", user.email.orNull)

        // Serialize generic record object into byte array
        val writer = new SpecificDatumWriter[GenericRecord](schema)
        val out = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(genericUser, encoder)
        encoder.flush()
        out.close()

        val serializedBytes: Array[Byte] = out.toByteArray()

        val data = new ProducerRecord[String, Array[Byte]](topic, serializedBytes)
        producer.send(data)
      }
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
  }

}
