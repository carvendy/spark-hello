package com.carvendy.spark.day02

import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.SparkContext
import java.io.ObjectOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

object TestWordCount {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("spark://kvm-5-118:7077")
    val conf = sparkConf.setAppName("Simple Application2")
    val sc = new SparkContext(conf)
    
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.defaultFS", "hdfs://kvm-5-118:9000/")
    hadoopConf.set("fs.defaultFS.name","hadoop")
    hadoopConf.set("fs.defaultFS.password","x+y-z=hadoop")
    
    val fileSystem = FileSystem.get(hadoopConf)
    val path = new Path("/home/hadoop/data/write-object")
    val oos = new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)))
    
    val model = "model"
    oos.writeObject(model)
    oos.close
    
    sc.stop()
  }
}