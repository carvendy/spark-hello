package com.carvendy.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestTempFile {
  
   val filePath = "count.txt";
  
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local").setAppName("hello")
    val sc = new SparkContext(conf)
    val url = TestCount.getClass.getClassLoader.getResource(filePath);
    val textFile = sc.textFile(url.getFile)
    val linesWithSpark = textFile.filter(line => line.contains("a"))
    println(linesWithSpark.count())
    
  }
}