package com.atguigu.qzpoint.streaming

import scala.collection.mutable.ListBuffer

object princtice1 {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4)
    val list1 = List(3,4,5,6)

    // 集合最小值
    println("min => " + list.min)
    // 集合最大值
    println("max => " + list.max)
    // 集合求和
    println("sum => " + list.sum)
    // 集合乘积
    println("product => " + list.product)
    // 集合简化规约
    println("reduce => " + list.reduce((x:Int,y:Int)=>{x+y}))
    println("reduce => " + list.reduce((x,y)=>{x+y}))
    println("reduce => " + list.reduce((x,y)=>x+y))
    println("reduce => " + list.reduce(_+_))
    // 集合简化规约(左)
    println("reduceLeft => " + list.reduceLeft(_+_))
    // 集合简化规约(右)
    println("reduceRight => " + list.reduceRight(_+_))
    // 集合折叠
    println("fold => " + list.fold(0)(_+_))
    // 集合折叠(左)
    println("foldLeft => " + list.foldLeft(0)(_+_))
    // 集合折叠(右)
    println("foldRight => " + list.foldRight(0)(_+_))
    // 集合扫描
    println("scan => " + list.scan(0)(_+_))
    // 集合扫描(左)
    println("scanLeft => " + list.scanLeft(0)(_+_))
    // 集合扫描(右)
    println("scanRight => " + list.scanRight(0)(_+_))
  }
}