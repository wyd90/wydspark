package com.wyd.spark.core.ipjoin

object IpUtil extends Serializable {

  def ip2Long(ip: String) = {
    val arr = ip.split("[.]")
    var ipNum = 0L

    for (str <- arr) {
      ipNum = str.toLong | ipNum << 8L
    }

    ipNum
  }

  def searchProvince(ipNum: Long, rule: Array[(Long, Long, String)]): String = {
    var low = 0
    var high = rule.length - 1

    while (high >= low) {
      val mid = (low + high) / 2
      val midRule = rule(mid)
      if (ipNum >= midRule._1 && ipNum <= midRule._2) {
        return midRule._3
      } else if (ipNum < midRule._1) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    return "未知"
  }

}
