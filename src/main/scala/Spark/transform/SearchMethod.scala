package Spark.transform

import org.apache.spark.rdd.RDD

class SearchMethod(query: String){
  // 过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 过滤出包含字符串的RDD
  def getMatch(rdd:RDD[String]) = {
    rdd.filter(isMatch)
  }

  // 过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]) = {
    rdd.filter( x => x.contains(query))
  }

  // 过滤出包含字符串的RDD
  def getMatch3(rdd: RDD[String]) = {
    val str = this.query  // 这一步是在Driver完成的, String是可序列化的，只有RDD的操作才在executer端执行
    rdd.filter(x => x.contains(str))
  }
}



