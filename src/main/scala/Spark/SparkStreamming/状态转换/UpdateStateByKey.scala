package Spark.SparkStreamming.状态转换

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UpdateStateByKey")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream = ssc.socketTextStream("node3", 9999)

    // HDFS上
    ssc.sparkContext.setCheckpointDir("./temp")

    val wordAndOne: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values:Seq[Int], status:Option[Int]) => {
      // 当前批次内容的计算
      val sum = values.sum
      // 取出状态信息中上一次状态
      val lastStatus = status.getOrElse(0)
      Some(sum + lastStatus)
    }

    // 有状态转换
    val wordAndCount = wordAndOne.updateStateByKey(update)

    wordAndCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
