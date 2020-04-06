package Spark.SparkStreamming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDQueue").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建RDD队列
    val seqToRdds = new mutable.Queue[RDD[Int]]()

    // 读取RDD队列创建DStream，默认是true，表示每次只拿一个，和放的速度无关
    // false和放的速度有关，本例是3秒
    val rddDStream: InputDStream[Int] = ssc.queueStream(seqToRdds, oneAtATime = false)

    // 累加结果
    rddDStream.reduce(_+_).print()

    ssc.start()

    // 创建一个队列，把生成的RDD放入队列中
    for(1 <- 1 to 5){
      seqToRdds += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
