package Spark.StructuredStreaming

import Spark.JDBCSink

object mySQL {
  val url = "jdbc:mysql://namenode:3306/test"
  val user = "root"
  val pwd = "Summer@2020"

  val write = new JDBCSink(url, user, pwd)

}
