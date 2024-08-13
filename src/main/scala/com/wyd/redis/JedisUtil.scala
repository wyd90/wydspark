package com.wyd.redis

import java.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisUtil {

  val config = new JedisPoolConfig
  config.setMaxIdle(10)
  config.setTestOnBorrow(true)
  config.setMaxTotal(20)

  val pool = new JedisPool(config, "192.168.56.101", 6379, 5000)

  def getConnection = pool.getResource

  def main(args: Array[String]): Unit = {
    val conn = getConnection
    val keys: util.Set[String] = conn.keys("*")

    val value: util.Iterator[String] = keys.iterator()
    while (value.hasNext) {
      val key: String = value.next()

      val str: String = conn.get(key)
      println(s"$key    $str")

//      conn.del(key)
    }

    conn.close()
  }

}
