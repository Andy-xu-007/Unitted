package Spark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topics = Array("first")
    val brokers = "namenode:9092,node1:9092,node3:9092"
    val groupID = "bigData"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupID,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val lineDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe(topics, kafkaParams)
    )

    lineDStream.foreachRDD(rdd => {
      if (rdd != null && !rdd.isEmpty()){
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(iterater => {
          if (iterater != null && iterater.nonEmpty){
            while (iterater.hasNext){
              val record: ConsumerRecord[String, String] = iterater.next()
              println(record.offset() + record.key() + record.value())
            }
          }
        })
        lineDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("commit offset !")
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
