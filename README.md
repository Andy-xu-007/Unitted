# Unitted
Hadoop programming on Scala, the version i used of Hadoop component are more higher:
  Hadoop: 3.2.0
  Hive:   3.1.1
  Hbase:  2.2.2
  Kafka:  2.3.1
  Zookeeper: 3.5.5
  
  Spark:  2.4.4
  spark-streaming-kafka-0-10_2.12 （the ）
  Warning:  Hive 3 on Spark 2.4.4 has a very serious bug while Using local IDEA to link the remote hive(VM), 
              no effect using the spark shell to link Hive with same machine
              https://www.cnblogs.com/fbiswt/p/11798514.html
  
  MySQL:  8.0.18
  
  重点：
    Hadoop: 
      高并发与负载均衡
      联邦 Federation
      Dock与Cgroup
      大数据中小文件存取
      md5
      has稳定算法，has join
      谓词下推
      计算向数据移动
      Pipeline
      Hadoop 2.x的MapReduce模块用的是java写的，3.x用的是nctive写的，直接调用底层C库
    Hive:
      调优
      内外表
      分区与分桶
      抽样
      行转列，列转行，explode
      窗口函数
      自定义函数 UDF, UDAF, UDTF
    Hbase:
      布隆过滤器
      两军问题、拜占庭将军问题
      LRU cache
      读比写慢
      Hbase过滤器
      预分区
    Kafka：
      解耦
      负载均衡，高并发
      LEO， HW
      幂等性
      顺序写磁盘，零拷贝
      --zookeeper  -> --bootstrap-server
    Spark:
      spark core, 独立调度器，Yarn, Mesos, spark SQL, Spark  Streaming，Spark Mlib，Spark GraghX
      迭代运算
      OOM
      DAG
      广播变量
      RDD -> DataFrame -> DataSet
      背压机制
      有/无状态转换
      窗口操作
      维度，粒度
      
      
      
      
      
      
      
      
      
      
      
      
      
