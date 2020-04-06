package Spark.SparkStreamming

import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Windows {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Windows")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineDStream = ssc.socketTextStream("node3", 9999)
    ssc.sparkContext.setCheckpointDir("E:\\workspace\\spark-file\\tmp")

    val wordAndOne = lineDStream.flatMap(_.split(" ")).map((_, 1))

    // 窗口操作（窗口大小6，滑动步长3
    val wordAndCount = wordAndOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      // (a: Int, b: Int) => a - b,   // invReduceFunc，上次的状态加上本次多出来的，再减去上次多出来的
      Seconds(6),
      Seconds(3)
    )

    wordAndCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
