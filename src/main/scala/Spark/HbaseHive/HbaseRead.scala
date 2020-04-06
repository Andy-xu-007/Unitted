package Spark.HbaseHive

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 所有 Spark 和 HBase 集成的基础是 HBaseContext。 HBaseContext 接受 HBase 配置并将其推送到 Spark Executor。
 *    我们可以在静态位置为每个 Spark Executor 建立一个 HBase 连接。
 *
 * Spark Executor 可以位于与 Region Server 相同的节点上, 也可以位于不依赖于共存位置的不同节点上。
 *    可以将每个 Spark Executor 看作一个多线程 client 应用程序。 这允许在 Executor 上运行的任何 Spark Task 访问共享连接对象。
 *
 * http://makaidong.com/cssdongl/228_1393736.html
 */
object HbaseRead {
  def main(args: Array[String]): Unit = {
    val tableName = "test"
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hbase")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    // 设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set(HConstants.ZOOKEEPER_QUORUM, "node1, node2, node3")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
//    conf.set(TableInputFormat.SCAN_COLUMNS, "cf1:arrivecity cf1:carno")  // 列
//    conf.set(TableInputFormat.SCAN_ROW_START, "20180517132443-Y01591805170032")  // 设置开始rowkey
//    conf.set(TableInputFormat.SCAN_ROW_STOP, "20180615170225-Y01591806150061")  // 设置终止rowKey ,范围是左闭区间右开区间
//    conf.set(TableOutputFormat.OUTPUT_TABLE, "test")  // 输出

    // 读取HBase数据创建RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],  // take
      classOf[ImmutableBytesWritable],  // k, hbase table rowKey
      classOf[Result]  // v, result set
    )

    // 打印
    hbaseRDD.foreach(x => {
      val value = x._2
      val cells = value.rawCells()
      cells.foreach(y => {
        println(Bytes.toString(CellUtil.cloneRow(y)))
      })
    })

    sc.stop()

  }
}
