package KafkaDemo

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

/**
 * 使用自定义分区
 * 1，可以在创建生产者的时候加入
 * 2，可以在new ProducerRecord加入
 * 3，也可以添加在配置信息里 -- 全类名
 */
object PartitionerProducer {
  def main(args: Array[String]): Unit = {
    // 创建kafka生产者的配置信息
    val props = new Properties()
    // 1，Kafka 服务端的主机名和端口号，"bootstrap.servers"替换为ProducerConfig就可以看到所有的参数
    props.put("bootstrap.servers","namenode:9092")
    // 2，ack的应答级别：等待所有副本节点的应答
    props.put("acks", "all")
    // 3，消息发送最大尝试次数，即重试次数
    props.put("retries", "1")
    // 4，一次发送消息的批次大小：16K
    props.put("batch.size", "16384")
    // 5，等待时间  --- 4,5同为限制条件
    props.put("linger.ms", "1")
    // 6，发送缓存区内存大小：RecordAccumulator缓冲区大小：32M
    props.put("buffer.memory", "33554432");
    // 7，key 序列化类
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    // 8，value 序列化类
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    // 9，添加自定义分区器, value 选中该class，右键选择copy reference，然后贴过来
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "KafkaDemo.partitioner")

    // 创建生产者对象
    val producer = new KafkaProducer[String, String](props)

    // 发送数据
    for (i <- 1 to 10) {
      producer.send(new ProducerRecord[String, String]("first","Andy -- " + i), new Callback {
        //回调函数，该方法会在Producer收到ack时调用，为异步调用
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            println(metadata.partition() + " - - - " + metadata.offset())
          } else {
            exception.printStackTrace()
          }
        }
      })
    }

    //关闭
    producer.close()
  }
}
