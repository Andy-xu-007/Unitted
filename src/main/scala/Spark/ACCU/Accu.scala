package Spark.ACCU


import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Accu {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Accu")
    val sc = new SparkContext(sparkConf)

    // 创建累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    // 创建RDD
    val rdd = sc.parallelize(1 to 5)

    val maped = rdd.map(x => {
      sum.add(x)
      x
    }
    )

    maped.cache()  // 防止下面有多个action算子，导致累加器被多次执行

    // collect返回的是RDD，不包含sum，所以要用累加器
    maped.collect.foreach(println)
    maped.foreach(println)

    println(" * * * ")

    println(sum.value)
    sc.stop()
  }
}
