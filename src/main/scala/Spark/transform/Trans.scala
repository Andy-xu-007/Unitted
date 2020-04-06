package Spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Trans {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Trans").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 创建search对象
    val search = new SearchMethod("a")

    // 创建RDD
    val words = sc.parallelize(Array("aa", "ab", "bb"))

    // 过滤出包含"a"的string
    val filtered: RDD[String] = search.getMatch3(words)
    // val filtered: RDD[String] = search.getMatch2(words)  // object SearchMethod 必须  extends Serializable

    filtered.foreach(println)
    sc.stop()
  }
}
