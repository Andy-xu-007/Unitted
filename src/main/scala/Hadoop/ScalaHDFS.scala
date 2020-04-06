package Hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

// https://blog.csdn.net/eyeofeagle/article/details/83547720

object ScalaHDFS {
  def main(args: Array[String]): Unit = {

    /**
     * 启动程序
     */
    readFile()
    write()
    cpFile()
  }
  // 配置,文件系统
  val conf = new Configuration()
  conf.set("fs.defaultFS","hdfs://local:9000")
  val fs = FileSystem.get(conf)

  // 读取文件
  def readFile() = {
    val in = fs.open(new Path("/people.txt"))
    val buf = new Array[Byte](1024)

    var len = in.read(buf)
    while (len != -1) {
      println(new String(buf, 0, len))
      len = in.read(buf)
    }
    // 关闭资源
    in.close()
  }

  // 写文件
  def write() = {
    val out = fs.create(new Path("/a.txt"), true)
    for (x <- 1 to 5) {
      out.writeBytes("hello," + x)
    }
    out.close()
  }

  // 上传，下载
  def cpFile() = {
    fs.copyFromLocalFile(new Path("/a.txt"), new Path("/opt/data/aaa.txt"))
    fs.copyToLocalFile(new Path("/a.txt"), new Path("/opt/data/aaa.txt"))
    fs.close()
  }
}
