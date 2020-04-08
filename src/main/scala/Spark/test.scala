package Spark

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("test")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_, 1)
      .sortBy(_._2, false)
      .saveAsTextFile(args(0))

    sc.stop()
  }
}
