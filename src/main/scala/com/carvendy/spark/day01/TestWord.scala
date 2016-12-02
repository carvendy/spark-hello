package com.carvendy.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestWord extends App {
  //println(1)

  val filePath = "target/classes/word.txt";
  val conf = new SparkConf().setMaster("local").setAppName("hello")
  val sc = new SparkContext(conf)

  val lines = sc.textFile(filePath)
  val lineLengths = lines.map(s => s.length)
 // println(lineLengths.persist())
  
  var it = lineLengths.toLocalIterator;
  while (it.hasNext) {
    println("line-count:" + it.next);
  }

  val totalLength = lineLengths.reduce((a, b) => a + b)
  println("total-count:" + totalLength)
  
  
  
}