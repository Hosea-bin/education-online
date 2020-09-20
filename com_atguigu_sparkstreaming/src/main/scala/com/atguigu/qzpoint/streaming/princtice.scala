package com.atguigu.qzpoint.streaming

import scala.collection.mutable.ListBuffer

object princtice {
  def main(args: Array[String]): Unit = {
    // 可变集合
    val buffer1 = ListBuffer(1,2,3,4)
    val buffer2 = ListBuffer(5,6,7,8)

    val buffer5: ListBuffer[Int] = buffer1 ++ buffer2
    val buffer6: ListBuffer[Int] = buffer1 ++= buffer2
    println(buffer5)
    println(buffer6)
    println(buffer5 eq buffer6)
println("--------")
    val set1 = Set(1,2,3,4)
    val set2 = Set(5,6,7,8)

    // 增加数据
    val set3: Set[Int] = set1 + 4 + 6
    val set4: Set[Int] = set1.+(6,7,8)
    println(set3)
    println(set4)
    println("-----------------")
    val set6: Set[Int] = set1 ++ set2
    set6.foreach(println)
    println("********")
    val set7: Set[Int] = set2 ++ set1
    set7.foreach(println)
    println(set6 eq set7)

  }
}