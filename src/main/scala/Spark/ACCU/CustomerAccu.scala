package Spark.ACCU

import org.apache.spark.{SparkConf, SparkContext}

object CustomerAccu {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomerAccu")
    val sc = new SparkContext(sparkConf)

    // 创建累加器
    val accu = new CustomerAccuMethod

    // 注册累加器
    val sum: Unit = sc.register(accu, "sum")

    // 创建RDD
    val rdd = sc.parallelize((1 to 5))

    val maped = rdd.map(x => {
      accu.add(x)
      x
    })
    maped.collect().foreach(println)
    println(" * * * * * ")

    // 打印累加器的值
    println(accu.value)
    sc.stop()
  }
}
