package KafkaDemo.Interceptor

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class CounterInterceptor extends ProducerInterceptor[String, String]{

  var success:Int = _
  var error:Int = _

  override def configure(configs: util.Map[String, _]): Unit = {

  }

  override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
    record
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata != null) {
      success += 1
    } else {
      error += 1
    }
  }

  // 最终的输出
  override def close(): Unit = {
    println("Success: " + success)
    println("Error: " + error)
  }

}
