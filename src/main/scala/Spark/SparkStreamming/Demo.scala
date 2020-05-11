package Spark.SparkStreamming

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

//node3 上输入 nc -lk 9999

object Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("Demo")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建DStreaming，由于需要通信，所以选择socket，nc -lk 9999
    val lineDStream = ssc.socketTextStream("node3", 9999)

    val wordAndCount: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordAndCount.foreachRDD(rdd => {
      // 旧版本是hdfs://namenode:9000/...
      // rdd.saveAsTextFile("hdfs:///...")
      rdd.foreachPartition(
      line => {
        // 下面两行涉及的参数在hive-site.xml文件里
        // Driver参数新旧版本不一样
        // 强制JVM将com.mysql.jdbc.Driver这个类加载入内存，以便将其注册到DriverManager类上去
            // 返回与带有给定字符串名的类或接口相关联的 Class 对象
        Class.forName("com.mysql.cj.jdbc.Driver")
        val conn = DriverManager.
          getConnection("jdbc:mysql://namenode:3306/test", "root", "Summer@2020")
        try {
          for ( row <- line) {
            val sql = "insert into tp4(titleName, count)values('" + row._1 + "','" + row._2 + "')"
            conn.prepareStatement(sql).executeUpdate()
          }
        } finally {
          conn.close()
        }
      }
    )}
    )
    // 触发计算
    // wordAndCount.print()

    // 开启流处理
    ssc.start()

    // 等待所有线程全部结束之后，再退出
    ssc.awaitTermination()
  }
}
