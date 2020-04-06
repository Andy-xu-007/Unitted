package Spark.SparkStreamming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//node3 上输入 nc -lk 9999

object Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStreaming，由于需要通信，所以选择socket，nc -lk 9999
    val lineDStream = ssc.socketTextStream("node3", 9999)

    val wordAndCount = lineDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // 触发计算
    wordAndCount.print()

    // 开启流处理
    ssc.start()

    // 等待所有线程全部结束之后，再退出
    ssc.awaitTermination()


  }
}
