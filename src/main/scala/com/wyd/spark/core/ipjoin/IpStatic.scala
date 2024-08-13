package com.wyd.spark.core.ipjoin

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object IpStatic {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ipstatic").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rulesArray = sc.textFile("D:\\yarnData\\ip\\rule")
      .map(line => {
        val arr = line.split("[|]")
        (arr(2).toLong, arr(3).toLong, arr(6))
      }).collect()

    val ruleBroadCast = sc.broadcast(rulesArray)

    val res = sc.textFile("D:\\yarnData\\ip\\input")
      .map(line => {

        val rule: Array[(Long, Long, String)] = ruleBroadCast.value

        val arr = line.split("[|]")
        val ipNum = IpUtil.ip2Long(arr(1))
        val province = IpUtil.searchProvince(ipNum, rule)
        (province, 1)
      }).reduceByKey(_ + _).sortBy(_._2, false).collect()

    println(res.toBuffer)
    sc.stop()
  }

}
