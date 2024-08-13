package com.wyd.spark.core.favt

object FavtImplicitUtil {

  implicit val favtOrder = new Ordering[((String, String), Int)] {
    override def compare(x: ((String, String), Int), y: ((String, String), Int)): Int = y._2 - x._2
  }

}
