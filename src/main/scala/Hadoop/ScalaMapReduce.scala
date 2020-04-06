package Hadoop

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.Tool

/**
 * Start
 */
object ScalaMapReduce extends Configured with Tool{
  override def run(args: Array[String]): Int = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val job = Job.getInstance(conf)
    job.setJarByClass(this.getClass)
    job.setJobName("wordCount")

    // Map, Reduce, Combiner
    // job.setMapperClass(classOf[Map])  // 初始化为自定义/Map类
    job.setCombinerClass(classOf[Red1])
    job.setReducerClass(classOf[Red1])  // 初始化为自定义Reduce类
    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])  //指定输出的Value的类型，Text相当于String类

    // Input, Output
    FileInputFormat.addInputPath(job,new Path("/people.txt"))
    FileOutputFormat.setOutputPath(job, new Path("/Andy_MR"))

    // 等待作业完成
    val status = job.waitForCompletion(true)
    // 返回值： int 【0完成, 1 失败】
    if (status) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    run(Array())
  }
}

/**
 * Mapper
 */
class Map1 extends Mapper[LongWritable, Text, Text, IntWritable] {
  // @throws(classOf[IOException])
  // @throws(classOf[InterruptedException])
  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val words = value.toString
    val arr = words.split(",")

    // 遍历所有单词
    for (x <- arr) {
      context.write(new Text(x), new IntWritable(1))
    }
  }
}

/**
 * Reducer
 */
class Red1 extends Reducer[Text, IntWritable, Text, IntWritable] {
  def reduce(key: Text,
             values:Iterable[IntWritable],
             context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
    // 统计个数
    var count = 0
    for (x <- values) {
      count += x.get()
    }
    context.write(key, new IntWritable(count))
  }
}