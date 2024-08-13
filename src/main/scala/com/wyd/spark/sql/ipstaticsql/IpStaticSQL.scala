package com.wyd.spark.sql.ipstaticsql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.LongType

object IpStaticSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ipstaticSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ruleDF = spark.read.textFile("D:\\yarnData\\ip\\rule")
      .map(line => {
        val arr = line.split("[|]")
        val start = arr(2).toLong
        val end = arr(3).toLong
        val p = arr(6)
        (start, end, p)
      }).toDF("r_start", "r_end", "r_p")

    val ip2longFunc = spark.udf.register("ip2long", (ip: String) => {
      val arr = ip.split("[.]")
      var ipNum = 0L

      for (str <- arr) {
        ipNum = str.toLong | ipNum << 8L
      }

      ipNum
    })

    val accessDF = spark.read.textFile("D:\\yarnData\\ip\\input")
      .map(line => {
        val arr = line.split("[|]")
        (arr(1))
      }).toDF("t_ip")

//    import org.apache.spark.sql.functions._
//    accessDF.select(callUDF("ip2long", $"t_ip") as "ip_number").join(broadcast(ruleDF), $"ip_number" >= $"r_start" and $"ip_number" <= $"r_end", "inner")
//      .select($"r_p" as "province")
//      .groupBy($"province").agg(count("*") as "cnts")
//      .orderBy($"cnts" desc).show()

    ruleDF.createOrReplaceTempView("t_rule")
    accessDF.createOrReplaceTempView("t_access")

    import spark.sql
    sql("select /*+BROADCAST(t_rule) */ r_p as province,count(*) as cnts from(select ip2long(t_ip) as ip_number from t_access)t1 join t_rule on ip_number >= r_start and ip_number <= r_end group by r_p order by cnts desc").show()

    spark.stop()
  }

}
