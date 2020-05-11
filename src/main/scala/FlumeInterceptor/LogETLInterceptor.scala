package FlumeInterceptor

import java.nio.charset.Charset
import java.util

import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class LogETLInterceptor extends Interceptor {
  override def initialize(): Unit = {

  }

  override def intercept(event: Event): Event = {
    // 清洗数据 ETL
    // 1，获取日志
    val body: Array[Byte] = event.getBody
    val log = new String(body, Charset.forName("UTF-8"))
    // 2，区分类型
    if (log.contains("start")){
      // 验证启动日志逻辑
      if (LogUtils.validateStart(log)){
        event
      }
    } else {
      // 验证时间日志逻辑
      if (LogUtils.validateEvent(log)){
        event
      }
    }
    null
  }

  def intercept(events: util.List[Event]): util.List[Event] = {
    val events_scala = events.asScala
    val interceptors = ArrayBuffer[Event]()
    // 多Event处理
    for (event: Event <- events_scala){
      val intercept1: Event = intercept(event)
      if (intercept1 != null){
        interceptors.append(intercept1)
      }
    }
    interceptors.asJava
  }

  override def close(): Unit = {

  }

  class Builder extends Interceptor.Builder{
    override def build(): Interceptor = {
      new LogETLInterceptor()
    }

    override def configure(context: Context): Unit = {

    }
  }

}


