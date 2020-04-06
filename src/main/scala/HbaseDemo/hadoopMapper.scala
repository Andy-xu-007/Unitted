package HbaseDemo

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, Mapper, OutputCollector, Reporter}

class hadoopMapper extends Mapper[LongWritable, Text,LongWritable, Text]{
  override def map(k1: LongWritable, v1: Text, outputCollector: OutputCollector[LongWritable, Text], reporter: Reporter): Unit = {

  }

  override def configure(jobConf: JobConf): Unit = {

  }

  override def close(): Unit = {

  }

}
