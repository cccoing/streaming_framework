package com.ys.streaming.profile

import java.util.ArrayList
/**
  * Created by hadoop on 2017/11/16.
  */
object IteratorTest {

  def main(args: Array[String]): Unit = {

    val bb = List(1, 2, 3, 4, 5)
    val cc = bb.iterator
    val dd = cc
    cc.foreach(println)
    println("-----")
    cc.foreach(println)
    println("-----")
    dd.foreach(println)

  }
}
