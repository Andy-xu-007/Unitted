package Spark.SQL

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object mySQLRDD {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("MySql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://namenode:3306/rdd"
    val userName = "root"
    val passWd = "Summer@2020"


    // 创建JDBC_RDD
    val jdbcRDD = new JdbcRDD(sc,
      () => {
        DriverManager.getConnection(url, userName, passWd)
      },
      "select name from event where ? <= id and id <= ?",
      1, 10,
      1,
      rs => rs.getInt(1)
    )

    jdbcRDD.foreach(println)
    sc.stop()
  }
}
