package com.damon.utils

import java.sql.{Connection, DriverManager}
import java.util
import java.util.Properties

/**
 * phoenix 连接工具类
 */
object PhoenixConnPool {
  lazy val properties: Properties = InitPropertiesUtil.initPhoenixProp
  private val maxConnection = Integer.parseInt(properties.getProperty("jdbc.max_connection"))
  private val connectionNum = properties.getProperty("jdbc.connection_num")
  private var currentNum = 0
  private var pools: util.LinkedList[Connection] = null       // 连接池
  private val url = properties.getProperty("jdbc.url")

  def initConnection(): Connection = {
    val connection = DriverManager.getConnection(url)
    connection.setAutoCommit(false)
    connection
  }

  private def initConnectionPool(): util.LinkedList[Connection] = {
    AnyRef.synchronized{
      pools = new util.LinkedList[Connection]()
      if (pools.isEmpty) {
        for (i <- 1 to connectionNum.toInt) {
          pools.push(initConnection())
          currentNum += 1
        }
      }
    }
    pools
  }

  /**
   * 获得连接
   */
  def getConnection(): Connection = {
    if (pools == null)
      initConnectionPool()
    if (pools.size() == 0)
      initConnection()
    else
      AnyRef.synchronized{
        pools.poll()
      }
  }

  def releaseConnection(connection: Connection): Unit = {
    pools.push(connection)
  }
}
