package Practice

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
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topics = Array("first")
    val brokers = "namenode:9092, node1:9092, node2:9092"
    val groupID = "bigData"




    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupID,
      "auto.offset.reset" -> "latest",
      "auto.offset.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
    )


    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe(topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      if (rdd != null && !rdd.isEmpty()){
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(iter => {
          while (iter.hasNext){
            val record: ConsumerRecord[String, String] = iter.next()
            println(record.offset() + record.key())
          }
        })
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println(" Commit offset os done !")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
