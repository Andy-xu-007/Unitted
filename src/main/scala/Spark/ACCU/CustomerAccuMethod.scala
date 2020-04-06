package Spark.ACCU

import org.apache.spark.util.AccumulatorV2

/**
 * 自定义累加器
 */
class CustomerAccuMethod extends AccumulatorV2[Int, Int]{

  //定义一个属性
  var sum: Int = 0

  // 判断对象是否为空，其实也是判断属性
  override def isZero: Boolean = sum == 0

  // 复制
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new CustomerAccuMethod
    accu.sum = this.sum
    accu
  }

  // 重置
  override def reset(): Unit = sum = 0

  // 添加值
  override def add(v: Int): Unit = sum += v

  // 合并Executor端传回来的数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = this.sum += other.value

  // 返回值
  override def value: Int = sum
}
