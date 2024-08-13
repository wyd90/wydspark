package com.wyd.spark.sql.favt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object FavtSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FavtSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.read.textFile("D:\\yarnData\\favt\\input")
    val subjectAndTeacherDF = lines.map(line => {
      val arr = line.split("[/]")
      val subject = (arr(2).split("[.]")) (0)
      val teacher = arr(3)
      (subject, teacher)
    }).toDF("subject", "teacher")

    import spark.sql
//    subjectAndTeacherDF.createOrReplaceTempView("sub_teacher")
//    val res = sql("select subject,teacher,cnts from(select subject,teacher,cnts,row_number() over(partition by subject order by cnts desc) rk from(select subject,teacher,count(*) cnts from sub_teacher group by subject,teacher)t)t2 where rk < 2")
//    res.show()

    import org.apache.spark.sql.functions._
    subjectAndTeacherDF.groupBy($"subject",$"teacher").agg(count("*") as "cnts")
        .select($"subject",$"teacher",$"cnts",row_number().over(Window.partitionBy($"subject").orderBy($"cnts" desc)) as "rk")
        .select($"subject", $"teacher",$"cnts")
        .where($"rk" === 1).show()

    spark.stop()
  }

}
