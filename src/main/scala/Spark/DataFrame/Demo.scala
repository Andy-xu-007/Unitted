package Spark.DataFrame

import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark = SparkSession
      .builder()
      .appName("Demo")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 创建一个DataFrame
    val dataFrame = spark.read.json("E:\\bigDataDoc\\data\\people.json")

    dataFrame.filter($"age" > 20).show()

    dataFrame.createTempView("people")

    spark.sql("select * from people").show()

    spark.stop()
  }
}
