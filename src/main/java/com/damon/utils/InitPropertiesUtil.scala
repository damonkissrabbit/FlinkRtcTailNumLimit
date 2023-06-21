package com.damon.utils

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties


// 自定义类继承 Serializable 接口的作用是使该类的对象能够在网络上进行序列化和反序列化操作
// 序列化是将对象转化为子接口的过程，以便在网络上插损胡或持久化存储
// 反序列化则是将子接口转换回对象的过程
// 在 Flink 中，当使用了自定义的类作为算子的输入、输出类型，或者在算子函数中使用该类的实例作为变量时
// 这个类需要实现 Serializable 接口，这是因为 Flink 在分布式计算中需要将对象在不同的节点之间传输，需要将对象序列化为字节流
// 然后在接收端进行反序列化
object InitPropertiesUtil extends Serializable {

  /**
   * Description: Configuration parameter initialization public class.
   * Demo:
   * lazy val prop: Properties = InitPropertiesUtil.initKafkaPro
   * val topicSet = prop.getProperty("topic").split(",").toSet
   * prop.getProperty("bootstrap_servers")
   *
   */
  def initBusinessProp: Properties = {
    val prop = new Properties()
    // getClass 是一个用于获取对象的运行时类的方法
    val in: InputStream = getClass.getResourceAsStream("/src/main/resources/business.properties")
    if (in == null) {
      println("Error: business's properties init failed in is null.")
    }
    prop.load(new BufferedInputStream(in))
    println("Success: business's properties init ok.")
    prop
  }


  // get kafka's properties
  def initKafkaProp: Properties = {
    val prop = new Properties()
    val in: InputStream = getClass.getResourceAsStream("/src/main/resources/kafka.properties")
    if (in == null) {
      println("Error: kafka's properties init failed in is null.")
    }
    prop.load(new BufferedInputStream(in))
    println("Success: kafka's properties init ok.")
    prop
  }

  def initRedisProp: Properties = {
    val prop = new Properties()
    val in: InputStream = getClass.getResourceAsStream("/src/main/resources/redis.properties")
    if (in == null) {
      println("Error: redis's properties init failed in is null.")
    }
    prop.load(new BufferedInputStream(in))
    println("Success: redis's properties init ok.")
    prop
  }

  def initPhoenixProp: Properties = {
    val prop = new Properties()
    val in: InputStream = getClass.getResourceAsStream("/src/main/resources/phoenix.properties")
    if (in == null) {
      println("Error: phoenix's properties init failed in is null.")
    }
    prop.load(new BufferedInputStream(in))
    println("Success: phoenix's properties init ok.")
    prop
  }

  def initBasicProp: Properties = {
    val prop = new Properties()
    val in: InputStream = getClass.getResourceAsStream("/src/main/resources/basic.properties")
    if (in == null) {
      println("Error: basic's properties init failed in is null.")
    }
    prop.load(new BufferedInputStream(in))
    println("Success: basic's properties init ok.")
    prop
  }

}
