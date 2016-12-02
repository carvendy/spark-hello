package com.carvendy.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkSimple {
  
    def main(args: Array[String]): Unit = {
      val logFile = "D:/tools/spark-2.0.0-bin-hadoop2.7/README.md" // Should be some file on your system
      val sparkConf = new SparkConf()
      //sparkConf.setMaster("spark://kvm-5-118:7077")
      sparkConf.setMaster("local")
      val conf = sparkConf.setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      
      val data = Array(1, 2, 3, 4, 5)
      val distData = sc.parallelize(data)
      sc.stop()
    }
    
}