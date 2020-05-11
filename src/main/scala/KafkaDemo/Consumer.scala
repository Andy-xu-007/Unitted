package KafkaDemo

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}

object Consumer {
  def main(args: Array[String]): Unit = {

    // 创建kafka消费者的配置信息
    val props = new Properties()
    //1，指定kafka服务器//指定kafka服务器
    props.put("bootstrap.servers", "namenode:9092, node1:9092, node2:9092")
    //2，消费组ID是test，控制台是默认分配的
    // 并非完全必需，它指定了消费者属于哪一个群组，但是创建不属于任何一个群组的消费者并没有问题
    props.put("group.id", "test")
    //3，以下两行代码 ---开启消费者自动提交offset值
    props.put("enable.auto.commit", "true")
    //4，自动确认offset的周期，自动提交的周期
    props.put("auto.commit.interval.ms", "1000")
    //5，设置key value的反序列化
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 6，offset重置，即从最开始读取topic，有使用条件，所以不一定生效，参数默认是latest
    // 组名没有换，同时offset没有失效，且消费者组已经消费过了，则下面的配置将失效
    // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // 定义consumer
    val consumer = new KafkaConsumer[String, String](props)
    val topic:String = "first"
    // 消费者订阅的 topic, 可同时订阅多个
    consumer.subscribe(Collections.singletonList(topic))

    //注意用标志位做循环判断
    var runnable = true
    while (runnable) {
      // 读取数据，读取超时时间为100ms，避免没有数据的一直在拉取，原版本poll中直接放入时间，具体区别在笔记
      // poll(Duration)这个版本修改了这样的设计，会把元数据获取也计入整个超时时间（更加的合理）
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(500))
      val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
      while (iter.hasNext) {
        val record: ConsumerRecord[String, String] = iter.next()
        println(record.offset() + "--" + record.key() + "--" + record.value())
      }
    }

  }
}
