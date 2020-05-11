package Spark.StructuredStreaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo")
      .master("local[2]")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "namenode")
      .option("port", "9999")
      .load()

    import spark.implicits._
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val wordCounts: DataFrame = words.groupBy("value").count()

    // 由于spark环境里嵌套了HDFS，所以需启动HDFS
    wordCounts.writeStream
      // 三种模式，complete，append, update
      // append不能用count()，append很少
        .outputMode("complete")
        .format("console")
        .start()

    spark.stop()

  }
}
