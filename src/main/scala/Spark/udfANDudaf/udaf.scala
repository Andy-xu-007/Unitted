package Spark.udfANDudaf

import org.apache.spark.sql.SparkSession

object udaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("udaf")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val dataFrame = spark.read.json("E:\\bigDataDoc\\data\\people.json")
    dataFrame.createTempView("people")

    // 注册UDAF
    spark.udf.register("myAvg", CustomerUDAFMethod)

    spark.sql("select myAvg(age) from people").show()

    spark.stop()
  }
}
