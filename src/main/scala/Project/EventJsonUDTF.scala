package Project

import java.util

import org.apache.commons.lang3.StringUtils
import  scala.util.control.Breaks._

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.json.{JSONArray, JSONException}


abstract class EventJsonUDTF extends GenericUDTF{

  override def initialize(argOIs: StructObjectInspector): StructObjectInspector = {
    // 定义UDTF返回值类型和名称
    // 下面的String是列名
    // 定义输出数据的列名
    val fieldNames: util.List[String] = new util.ArrayList[String]()
    // UDTF返回值的名称
    fieldNames.add("event_name")
    fieldNames.add("event_json")

    // 下面的String是当前函数的返回值，即上面的列以什么形式输出的，javaStringObjectInspector只是其中的一种
    // 定义输出数据的类型
    // 按照上面的名称对应的顺序
    val fieldType: util.List[ObjectInspector] = new util.ArrayList[ObjectInspector]()
    fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)


    // fieldNames是输出的字段名
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldType)
  }
  override def process(args: Array[AnyRef]): Unit = {
    // 传入的是json array => UDF 传入et

    // 获取数据
    val input = args(0).toString

    //合法校验
    if (!StringUtils.isBlank(input)){
      val ja: JSONArray = new JSONArray(input)
      if (ja != null){
        // 循环遍历array当中每一个元素，封装成返回的事件名称和事件内容
        for (i <- 0 to ja.length){
          val result = new Array[String](2)
//          {
//            "ett": "1506047605364",
//            "en": "display",
//            "kv": {
//              "goodsid": "236",
//              "action": "1",
//              "extend1": "1",
//              "place": "2",
//              "category": "75"
//            }
//          }
          try{
            // 取出每个的事件名称（ad/facoriters）
            result(0) = ja.getJSONObject(i).getString("en")
            // 取出每一个事件整体
            result(1) = ja.getString(i)
          } catch {
            case e: JSONException => {
              println(e)
            }
              // 写出
              forward(result)
          }

        }
      }

    }
  }

  override def close(): Unit = {

  }
}
