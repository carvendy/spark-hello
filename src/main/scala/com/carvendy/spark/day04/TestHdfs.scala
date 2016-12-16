package com.carvendy.spark.day04

import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.ObjectOutputStream
import java.io.FileInputStream
import java.io.ObjectInputStream

object TestHdfs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf //.setMaster("local[*]")
      .setMaster("spark://kvm-5-118:9000")
      .setAppName("hello")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.defaultFS", "hdfs://kvm-5-118:9000/")
    val fileSystem = FileSystem.get(hadoopConf)
    println(fileSystem)
    
   // val path = new Path("/user/hadoop/data/mllib/word2vec-object")
    
  //  val oos = new FSDataOutputStream(fileSystem.create(path))
    
    
   /* val oos = new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)))
    oos.writeObject(model)*/
    
    //val ois = new ObjectInputStream(new FileInputStream("/tmp/word2vec-object"))
    

    /*val output = new Path("hdfs://kvm-5-118:9000/data/")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI("hdfs://kvm-5-118:9000"), new org.apache.hadoop.conf.Configuration())

    //if (hdfs.exists(output)) hdfs.delete(output, true)
    val fs = hdfs.listStatus(output)
    val listPath = FileUtil.stat2Paths(fs)
    //val listPath = FileUtil.stat2Paths(fs)
    for (p <- listPath) println(p)*/

  }
}