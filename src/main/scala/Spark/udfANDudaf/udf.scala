package Spark.udfANDudaf

import org.apache.spark.sql.SparkSession

object udf {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("udf")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    val dataFrame = spark.read.json("E:\\bigDataDoc\\data\\people.json")

    spark.udf.register("addName", (x: String) => "Name: " + x)
    dataFrame.createTempView("people")
    spark.sql("select addName(name) from people").show()
  }
}
