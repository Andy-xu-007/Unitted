package HiveDemo

import java.util

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}

/**
 * 将以分隔符隔开的单词串split成一个个的单词
 */
class MyUDTF extends GenericUDTF{

  // 定义一个全局的集合
  private val dataList = new util.ArrayList[String]()

  override def initialize(argOIs: StructObjectInspector): StructObjectInspector = {

    // 下面的String是列名world
    // 定义输出数据的列名
    val fieldNames: util.List[String] = new util.ArrayList[String]()
    fieldNames.add("world")

    // 下面的String是当前函数的返回值，即上面的列以什么形式输出的，javaStringObjectInspector只是其中的一种
    // 定义输出数据的类型
    val fieldOIs: util.List[ObjectInspector] = new util.ArrayList[ObjectInspector]()
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    // fieldNames是输出的字段名
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  override def process(args: Array[AnyRef]): Unit = {

    // 1，获取数据
    val data = args(0).toString
    // 2，获取分割符
    val splitKey = args(1).toString
    // 3, 切分数据
    val words = data.split(splitKey)
    // 4, 遍历写出
    for (word <- words) {
      // 5，将数据放置在集合，以呼应方法initialize中的集合
      dataList.clear()
      dataList.add(word)
      // 6, 写出数据的操作
      forward(dataList)

    }

  }

  override def close(): Unit = {

  }
}
