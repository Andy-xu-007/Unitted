package Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    // 创建SparkConfig，打包到集群的时候，不要加setMaster("local[*]")这个方法，会在submit的时候指定的
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    // 创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 读取一个文件
    val line: RDD[String] = sc.textFile("E:\\bigDataDoc\\data\\word.txt")
    // val line1 = sc.textFile(args(0))  // 打包到集群的时候，动态的传进来

    // 压平 => 元祖 => 按K聚合
    val wc = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 保存到文件，该文件夹必须不存在，并且由于权限问题，不要放在C盘的User目录下
    wc.saveAsTextFile("E:\\bigDataDoc\\output")

    // 退出
    sc.stop()

  }
}
