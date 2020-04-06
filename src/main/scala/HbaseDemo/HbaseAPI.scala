package HbaseDemo

object HbaseAPI {
  def main(args: Array[String]): Unit = {
    val hbase = new HbaseUtil  // close方法最后用，所有admin，connection的close要放在最后
    hbase.putData("test","row1","cf", "a","today")
    hbase.close()
  }
}

