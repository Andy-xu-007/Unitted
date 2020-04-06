package Spark.SparkStreamming.状态转换

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 无状态操作
 */

object noStateTransForm {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("noStateTransForm")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStream
    val lineDStream = ssc.socketTextStream("node3", 9999)

    // 转换为RDD操作，但是返回值还是DStream，下面的flatMap之后的算子是RDD，之前的flatMap算子是DStream的
    val wordAndCountDStream = lineDStream.transform(rdd => {
      val value = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      value
    })
    wordAndCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
