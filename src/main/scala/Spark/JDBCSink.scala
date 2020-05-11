package Spark

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[Row]{

  // val driver = "com.mysql.jdbc.Driver"  // 旧版本
  val driver = "com.mysql.cj.jdbc.Driver"
  var connection: Connection = _
  var statement:Statement = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    val statement = connection.createStatement()
    true
  }


  def process(value: Row): Unit = {
    val searchName = value.getAs("titleName").toString.replaceAll("[\\[\\]]", "")
    val resultSet = statement.executeQuery("select 1 from test " +
        "where titleName = '" + searchName + "'")
    if(resultSet.next()){
      statement.executeUpdate("UPDATE test SET count=" + value.getAs("count") +
          "where titleName = '"+searchName+"'")
    }else{
      statement.executeUpdate("INSERT INTO test values('"+searchName+"', " +
        ""+value.getAs("count")+")")
    }

    // 或者简单一些
    statement.execute(s"insert into stream_word values ('${value.get(0)}',${value.get(1)})")

  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }

}
