package FlumeInterceptor

import org.apache.commons.lang3.math.NumberUtils

object LogUtils {

  // 验证启动日志
  // {...}
  def validateEvent(log: String): Boolean = {
    if (log == null){
      return false
    }
    if (!log.trim.startsWith("{") || !log.trim.endsWith("}")){
      return false
    }
    true
  }

  // 验证事件日志
  def validateStart(log: String): Boolean = {
    //15248548|{...}
    // 服务器实际爱你 | 日志内容
    if (log == null){
      return false
    }

    // 切割
    val logContent: Array[String] = log.split("\\|")
    if (logContent.length != 2){
      return false
    }

    // 校验服务器时间(长度必须是13位，必须全部是数字)
    if (logContent(0).length != 13 || NumberUtils.isDigits(logContent(0))){
      return false
    }

    // 校验日志格式
    if (!logContent(1).trim.startsWith("{") || !logContent(1).trim.endsWith("}")){
      return false
    }

    false
  }

}

