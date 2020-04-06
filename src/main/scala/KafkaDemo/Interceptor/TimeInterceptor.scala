package KafkaDemo.Interceptor

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class TimeInterceptor extends ProducerInterceptor[String,String]{

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {

    // 取出数据
    val value = record.value()
    // 创建一个新的ProducerRecord对象，并返回
    new ProducerRecord[String, String](record.topic(), record.partition(), record.key(), System.currentTimeMillis() + " - - - " + value)
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {}

  override def close(): Unit = {}

}
