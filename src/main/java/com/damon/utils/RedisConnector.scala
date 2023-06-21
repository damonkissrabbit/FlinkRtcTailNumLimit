package com.damon.utils

import redis.clients.jedis.{HostAndPort, JedisCluster}

import java.util
import java.util.Properties

object RedisConnector {
  val redisProp: Properties = InitPropertiesUtil.initRedisProp
  val ips: Array[String] = redisProp.getProperty("ips_cluster").split(",")
  val ports: Array[String] = redisProp.getProperty("ports_cluster").split(",")

  val jedisClusterNodes = new util.HashSet[HostAndPort]()

  for (i <- ips.indices) {
    for (j <- ports.indices){
      jedisClusterNodes.add(new HostAndPort(ips(i), ports(j).toInt))
    }
  }

  val clients = new JedisCluster(jedisClusterNodes, 15000)
}
