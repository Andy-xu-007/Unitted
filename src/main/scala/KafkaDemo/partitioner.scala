package KafkaDemo

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

// 自定义分区，具体根据业务展开
class partitioner extends Partitioner{
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    0  // 此处返回的是几号分区
  }

  override def close(): Unit = {

  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}
