package com.wyd.spark.sql.onhive

import org.apache.spark.sql.{SaveMode, SparkSession}

object OnHiveTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val spark = SparkSession
      .builder()
      .appName("on_hive_test")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

//    sql("load data local inpath 'file:///D:/yarnData/favt/input' into table t_teacher")
//    sql("select split(split(line, '\\\\/')[2], '\\\\.')[0] as subject, split(line, '\\\\/')[3] as teacher from  t_teacher")
//        .createOrReplaceTempView("t_mid")
//    sql("select subject,teacher,cnts from(select subject,teacher,cnts,row_number() over(partition by subject order by cnts desc) rk from(select subject,teacher,count(*) cnts from t_mid group by subject,teacher)t)t1 where rk = 1")
//        .show()
    sql("create table t_res(sex string, avgscore double) stored as parquet")
    spark.table("t_res").createOrReplaceTempView("t_res")
    sql("insert into table t_res select sex,avg(score) as avgscore  from t_ex_hbase_people group by sex")

    spark.stop()
  }

}
