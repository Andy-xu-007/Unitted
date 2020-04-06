package HiveDemo

import java.util

import org.apache.hadoop.hive.metastore.api.Date
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

class MyUDFhigher extends GenericUDF{

  var mapTasks: Int = _
  var init:String = _
  // 存放返回的list
  var ret = new util.ArrayList[Int]()

  // 这个方法只调用一次，并且在evaluate()方法之前调用，该方法接收的参数是一个ObjectInspectors数组，该方法检查接收正确的参数类型和参数个数
  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    println(new Date() + "#### initialize")
    // 初始化文件系统
    init = "init"
    // 定义函数的返回类型为java的list
    var returnOi: ObjectInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT)
    ObjectInspectorFactory.getStandardListObjectInspector(returnOi)
  }

  // 这个方法类似evaluate方法，处理真实的参数，返回最终结果
  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    ret.clear()
    "abc"
  }

  //此方法用于当实现的GenericUDF出错的时候，打印提示信息，提示信息就是该方法的返回的字符串
  override def getDisplayString(children: Array[String]): String = {
    "abc"
  }
}
