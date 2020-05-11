package Spark.udfANDudaf

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.ForeachWriter

import scala.collection.mutable

class KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)]{

  val props = new Properties()
  props.put("bootstrap.servers", servers)
  props.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  val result = new mutable.HashMap[String, String]
  var producer: KafkaProducer[String, String] = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    producer = new KafkaProducer(props)
    true
  }

  override def process(value: (String, String)): Unit = {
    producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }
}
