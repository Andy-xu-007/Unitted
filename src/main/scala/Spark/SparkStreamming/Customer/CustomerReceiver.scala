package Spark.SparkStreamming.Customer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CustomerReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomerReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val customerReceiverDStream = ssc.receiverStream(new CustomerReceiverMethod("node3", 9999))
    customerReceiverDStream.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
