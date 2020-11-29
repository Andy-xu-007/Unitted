import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object SparkHDFSHBase {
  private val conf: SparkConf = new SparkConf().setAppName("SparkHDFSHBase").setMaster("local[*]")
  conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
  conf.set("spark.network.timeout", "300")
  conf.set("spark.streaming.unpersist", "true")
  conf.set("spark.scheduler.listenerbus.eventqueue.size", "100000")
  conf.set("spark.storage.memoryFraction", "0.5")
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.shuffle.file.buffer", "64")
  conf.set("spark.shuffle.memoryFraction", "0.3")
  conf.set("spark.reducer.maxSizeInFlight", "24")
  conf.set("spark.shuffle.io.maxRetries", "60")
  conf.set("spark.shuffle.io.retryWait", "60")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  private val sc = new SparkContext(conf)

  private val configuration: Configuration = HbaseDemo.HBaseTools.HBaseConnection("test", "f", "a1,a2")
  sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
}
