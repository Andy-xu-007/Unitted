package Spark.Partitioner

import Spark.transform.SearchMethod
import org.apache.spark.{SparkConf, SparkContext}

object TestPartitioner {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Trans").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 创建search对象
    val search = new SearchMethod("a")

    // 创建RDD
    val words = sc.parallelize(Array("aa", "ab", "bb"))

    // 变为KV  RDD
    val wordAndOne = words.map((_,1))

    // 查看当前分区数据分布
    val valueWithIndex = wordAndOne.mapPartitionsWithIndex((i, items) => items.map((i, _)))

    valueWithIndex.foreach(println)

    // 重新分区
    val repartitioned = wordAndOne.partitionBy(new CustomerPartitioner(3))

    repartitioned.mapPartitionsWithIndex((i, items) => items.map((i, _))).foreach(println)

    sc.stop()

  }
}
