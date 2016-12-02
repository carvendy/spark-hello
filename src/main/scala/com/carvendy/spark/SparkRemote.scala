package com.carvendy.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkRemote {

  def main(args: Array[String]): Unit = {
    val logFile = "D:/tools/spark-2.0.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val sparkConf = new SparkConf()
    sparkConf.setMaster("spark://kvm-5-118:7077")
  //  sparkConf.setMaster("spark://192.168.5.118:7077")
    //sparkConf.setMaster("local")
    //sparkConf.setMaster("local[10]")
    val conf = sparkConf.setAppName("Simple Application2")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }

}