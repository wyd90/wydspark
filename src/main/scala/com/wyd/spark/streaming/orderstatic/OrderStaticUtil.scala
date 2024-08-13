package com.wyd.spark.streaming.orderstatic

import java.util.regex.Pattern

import com.wyd.redis.JedisUtil
import com.wyd.spark.core.ipjoin.IpUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object OrderStaticUtil {

  val checkpointDir = "file:///D:\\sparkck"

  val rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}"
  val pattern: Pattern = Pattern.compile(rexp)

  def judgeIp(ip: String) = {
    val matcher = pattern.matcher(ip)
    matcher.matches()
  }

  def etlData(lines: RDD[String]) = {
    lines.map(line => {
      val arr = line.split(" ")
      var flag = true
      try {
        if (arr.length == 6) {
          if (!OrderStaticUtil.judgeIp(arr(1))) {
            flag = false
          }
          arr(4).toDouble
          arr(5).toInt
        } else {
          flag = false
        }
      } catch {
        case _: Exception => flag = false
      }

      if (flag) {
        (arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toInt)
      } else {
        ("NULL", "NULL", "NULL", "NULL", -1d, -1)
      }
    }).filter(x => if (!x._1.equals("NULL")) true else false)
  }

  def calculateGMV(filtered: RDD[(String, String, String, String, Double, Int)]) = {
    val sum = filtered.map(x => x._5 * x._6).sum()
    val conn = JedisUtil.getConnection
    conn.incrByFloat("gmv", sum)
    conn.close()
  }

  @volatile private var instance: Broadcast[Array[(Long, Long, String)]] = null

  def getInstance(sc: SparkContext): Broadcast[Array[(Long, Long, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val rules: Array[(Long, Long, String)] = sc.textFile("file:///D:\\yarnData\\ip\\rule")
            .map(line => {
              val arr = line.split("[|]")
              (arr(2).toLong, arr(3).toLong, arr(6))
            }).collect()

          instance = sc.broadcast(rules)
        }
      }
    }
    instance
  }

  def calculateGMVByProvince(filtered: RDD[(String, String, String, String, Double, Int)], sc: SparkContext) = {
    val broadcastValue = getInstance(sc)
    val maped = filtered.map(x => {
      val ipNum = IpUtil.ip2Long(x._2)
      val rules = broadcastValue.value
      val province = IpUtil.searchProvince(ipNum, rules)
      (province, x._5 * x._6)
    })

    maped.reduceByKey(_+_).foreachPartition(it => {
      val conn = JedisUtil.getConnection
      it.foreach(x => {
        conn.incrByFloat(x._1, x._2)
      })
      conn.close()
    })
  }

}
