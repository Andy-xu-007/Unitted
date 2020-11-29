package Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计出每一个省份广告被点击次数的TOP3
 *    数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割
 *    1516609143867 6 7 64 16
 */
object adRank {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adRank").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("E:\\bigDataDoc\\data\\agent.log")

    // 切分提取出省份加广告((Province,AD),1)
    val provinceAndADToOne: RDD[((String, String), Int)] = lines.map(x => {
      val fields = x.split(" ")
      ((fields(1), fields(4)), 1)
    })

    // 统计省份广告被点击的总次数：((Province,AD),16)
    val provinceAndADToCount = provinceAndADToOne.reduceByKey(_+_)

    // 维度转换：((Province,AD),16) =》 (Province, (AD,16))
    val provinceToADAndCount = provinceAndADToCount.map(x =>
      (x._1._1, (x._1._2, x._2)))

    // 将同一个省份不同的广告做聚合，(Province, ((ad1,16), (ad2, 45),,,))
    val provinceToGroup: RDD[(String, Iterable[(String, Int)])] = provinceToADAndCount.groupByKey()

    // 排序，同时取前三，scala操作
    val top3: RDD[(String, List[(String, Int)])] = provinceToGroup.mapValues(x => {
      x.toList.sortWith((a, b) => a._2 > b._2).take(3)
      // x.toList.sortBy(_._2).take(3)
    })

    top3.collect().foreach(println)

    sc.stop()
  }
}
