package Project

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.json.JSONObject

abstract class BaseFieldUDF extends UDF {
  def evaluate(line: String, key: String): String ={
    // 切割
    val log: Array[String] = line.split("\\|")
    //合法性判断
    if(log.length != 2 || StringUtils.isBlank(log(1).trim)){
      ""
    }
    // 获取对象
    val json = new JSONObject(log(1).trim)
    var result = ""

    // 根据传入的取值，获取时间
    if("st".equals(key)){
      // 返回服务器时间
      result = log(0).trim
    }else if ("et".equals(key)){
      // 获取et
      if(json.has("et")){
        // 这里解析出来的是数组，所以用getString转换成整体对象返回了，需要用UDTF进一步处理
        result = json.getString("et")
      }
    }else if(json.has("cm")){
      // 获取cm对应的value，这里解析出来已经是一个JSON对象了
      val cm = json.getJSONObject("cm")
      if(cm.has(key)){
        result = cm.getString(key)
      }

    }
    result
  }
}

object BaseFieldUDF{
  def main(args: Array[String]): Unit = {
//    val patten = new Regex("\\n")
//    val str: String = "1540934156385|{\n\"ap\": \"gmall\",\n\"cm\": {\n\"uid\": \"1234\",\n\"vc\": \"2\",\n\"vn\": \"1.0\",\n\"la\": \"EN\",\n\"sr\": \"\",\n\"os\": \"7.1.1\",\n\"ar\": \"CN\",\n\"md\": \"BBB100-1\",\n\"ba\": \"blackberry\",\n\"sv\": \"V2.2.1\",\n\"g\": \"abc@gmail.com\",\n\"hw\": \"1620x1080\",\n\"t\": \"1506047606608\",\n\"nw\": \"WIFI\",\n\"ln\": 0\n},\n\"et\": [\n{\n\"ett\": \"1506047605364\",\n\"en\": \"display\",\n\"kv\": {\n\"goodsid\": \"236\",\n\"action\": \"1\",\n\"extend1\": \"1\",\n\"place\": \"2\",\n\"category\": \"75\"\n}\n},{\n\"ett\": \"1552352626835\",\n\"en\": \"active_background\",\n\"kv\": {\n\"active_source\": \"1\"\n}\n}\n]\n}\n}\n}"
//    val s = patten.replaceAllIn(str,"")

    val str = "1583776223469|{\"cm\":{\"ln\":\"-48.5\",\"sv\":\"V2.5.7\",\"os\":\"8.0.9\",\"g\":\"6F76AVD5@gmail.com\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"3\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1583707297317\",\"la\":\"-52.9\",\"md\":\"sumsung-18\",\"vn\":\"1.2.4\",\"ba\":\"Sumsung\",\"sr\":\"V\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1583705574227\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"0\",\"category\":\"63\"}},{\"ett\":\"1583760986259\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"4\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1583746639124\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"111839\",\"entry\":\"1\",\"action\":\"5\",\"contentType\":\"0\"}},{\"ett\":\"1583758016208\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583694079866\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1583699890760\",\"en\":\"favorites\",\"kv\":{\"course_id\":4,\"id\":0,\"add_time\":\"1583730648134\",\"userid\":7}}]}"
    val result = new BaseFieldUDF {}.evaluate(str, "et")
    println(result)
  }
}