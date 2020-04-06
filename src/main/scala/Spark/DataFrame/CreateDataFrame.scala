package Spark.DataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateDataFrame")
      .getOrCreate()

    // 获取sparkContext对象
    val sc = spark.sparkContext
    import spark.implicits._

//    /**
//     * 第一种
//     */
    // 创建RDD
//    val rdd = sc.parallelize(1 to 4)
    // 因为createDataFrame里参数需要rddRow，所以需要创建
//    val rddRow: RDD[Row] = rdd.map(x => Row(x))
    // 创建样例类  structType：定义列名和数据类型
//    val structType = StructType(StructField("id", IntegerType) :: Nil)
//    val dataFrame = spark.createDataFrame(rddRow, structType)
//    dataFrame.show()

    // 第二种
//    val rdd: RDD[String] = sc.textFile("E:\\workspace\\spark-file\\people.txt")
//    val rdd_ds: RDD[(String, Int)] = rdd.map(x => {
//      val para = x.split(" ")
//      (para(0), para(1).trim.toInt)
//    })
//    val df = rdd_ds.toDF("name","age")
//    df.show()

    /**
     * 第三种
     */

    val rdd = sc.textFile("E:\\bigDataDoc\\data\\people.txt")
    val rdd2DF = rdd.map(x => {
      val para = x.split(",")
      People(para(0), para(1).trim.toInt)
    })

    val df: DataFrame = rdd2DF.toDF
    df.show()

    spark.stop()
  }
}

// case class不能放在上面的main里面，定义需要放在方法的作用域之外（即Java的成员变量位置)
case class People(name:String, age:Int)
