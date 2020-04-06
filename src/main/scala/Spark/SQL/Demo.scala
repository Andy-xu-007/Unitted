package Spark.SQL

import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val dataFrame = spark.read.json("E:\\workspace\\spark-file\\people.json")

    // DSL，默认show 20 行
    dataFrame.filter($"age" > 20).show

    // SQL 风格
    // 创建临时表
    dataFrame.createTempView("people")
    // 执行查询
    spark.sql("select * from people")

    spark.stop()
  }
}
