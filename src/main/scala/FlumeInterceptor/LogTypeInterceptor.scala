package FlumeInterceptor

import java.nio.charset.Charset
import java.util

import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class

LogTypeInterceptor extends Interceptor{
  override def initialize(): Unit = {

  }

  override def intercept(event: Event): Event = {
    // 区分类型 start event
    // body header

    // 获取body
    val body: Array[Byte] = event.getBody
    val log: String = new String(body, Charset.forName("UTF-8"))

    // 获取header info
    val headers: util.Map[String, String] = event.getHeaders

    // 业务逻辑判断
    if (log.contains("start")){
      headers.put("topic", "topic_start")
    }else {
      headers.put("topic", "topic_event")
    }

    event
  }

  override def intercept(events: util.List[Event]): util.List[Event] = {
    val events_scala = events.asScala
    val intercepts = new ArrayBuffer[Event]()
    for(event <- events_scala){
      val intercept1 = intercept(event)
      intercepts.append(intercept1)
    }
    intercepts.asJava
  }

  override def close(): Unit = {

  }

  class Builder extends Interceptor.Builder{
    override def build(): Interceptor = {
      new LogTypeInterceptor
    }

    override def configure(context: Context): Unit = {

    }
  }
}
