package Spark.StructuredStreaming

import org.apache.spark.sql.SparkSession

object kafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("kafka")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "namenode:9092, node1:9092, node2:9092")
      .option("subscribe", "first")
      .load()
    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    val words = lines.flatMap(_.split(" "))

    val wordAndCount = words.groupBy("value").count()

    val query = wordAndCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
