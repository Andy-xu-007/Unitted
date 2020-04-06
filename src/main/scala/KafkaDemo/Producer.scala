package KafkaDemo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
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

    // 创建生产者对象
    val producer = new KafkaProducer[String, String](props)

    // 发送数据
    for (i <- 1 to 10) {
      producer.send(new ProducerRecord[String, String]("first",0,"sun", "Andy -- " + i))
    }

    //关闭
    producer.close()

  }
}
