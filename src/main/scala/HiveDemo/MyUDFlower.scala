package HiveDemo

import org.apache.hadoop.hive.ql.exec.UDF

class MyUDFlower extends UDF{

  def evaluate(data:Int): Unit = {
    data + 5
  }
}
