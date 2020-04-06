package Spark.Partitioner

import org.apache.spark.Partitioner

class CustomerPartitioner(partitions: Int) extends Partitioner{

  // 获取分区数
  override def numPartitions: Int = partitions

  // 获取分区号,不能按照value分区
  override def getPartition(key: Any): Int = 0
}
