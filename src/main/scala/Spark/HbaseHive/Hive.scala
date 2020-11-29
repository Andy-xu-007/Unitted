package Spark.HbaseHive

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 有个大bug，spark2.4.4与hive3的结合，需要在源码中删除HIVE_STATS_JDBC_TIMEOUT以及另外一个参数，然后重新编译spark-hive的jar包
 *  https://www.cnblogs.com/fbiswt/p/11798514.html
 */

object Hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Hive-demo")
      .enableHiveSupport()
      .getOrCreate()

    // 此时解析的是本地数据，在spark-warehouse文件夹下
    // 需要把hive-site.xml，core-site.xml，hdfs-site.xml放到resources文件夹下
    spark.sql("show tables").show()
    val dataSet: Dataset[Row] = spark.sql("show tables")



    spark.stop()

  }
}
