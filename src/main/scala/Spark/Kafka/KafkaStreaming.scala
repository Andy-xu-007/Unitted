package Spark.Kafka


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 如果将任务提交到集群中，那么要去掉.setMaster("local[2]")
    val sparkConf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // kafka参数说明
    val brokers = "namenode:9092,node1:9092,node2:9092"
    val topics = Array("first")
    val groupID = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    // 一下是基于spark-streaming-kafka-0-8的视频代码
    /*    val kafkaParams = Map(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
          ConsumerConfig.GROUP_ID_CONFIG -> groupID,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: lang.Boolean)
        )*/
    // 一定要加类型
    val kafkaParams= Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupID,
      // 指定从何处更新，latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
      "auto.offset.reset" -> "earliest",
      // 如果true, 则consumer定期地往zookeeper写入每个分区的offset, false: java.lang.Boolean
      // 消费kafka中的偏移量自动维护: kafka 0.10之前的版本自动维护在zookeeper  kafka 0.10之后偏移量自动维护topic(__consumer_offsets)
      "enable.auto.commit" -> (false: java.lang.Boolean),
//      // 序列化
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization,
//      //上游数据是avro格式的，配置avro schema registry解析arvo数据
//      "schema.registry.url" -> "namenode:8081",
      "key.deserializer" -> classOf[StringDeserializer],
      //值是avro数据，需要通过KafkaAvroDeserializer反序列化
      "value.deserializer" -> classOf[StringDeserializer]
    )

    // 读取Kafka数据创建DStreaming, kafka010版本的API
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      // 持久化策略
      PreferConsistent,
      // 订阅消息
      Subscribe(topics,kafkaParams))
    val wordAndCount: DStream[(String, Long)] = stream.map(_.value()).flatMap(_.split(" ")).map((_, 1L)).reduceByKey(_ + _)
    // 下面这一行没有数据生成
    wordAndCount.print()

    // 处理每个微批的rdd
    stream.foreachRDD(rdd => {
      if(rdd != null && !rdd.isEmpty()){
        // 获取每一个分区的消费的offset
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 对每个分区分别处理
        rdd.foreachPartition(iterator => {
          if(iterator != null && iterator.nonEmpty){
            // 作相应处理
            while(iterator.hasNext){
              //处理每一条记录
              val record: ConsumerRecord[String, String] = iterator.next()
              // 这个就是接受到的数值对象
              println(record.offset() + "--" + record.key() + "--" + record.value())
              // 可以插入数据库，或者输出到别的地方
            }
          }
        })
        // 手动提交偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("submit offset!")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
