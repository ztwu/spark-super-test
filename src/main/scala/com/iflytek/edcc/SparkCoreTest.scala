package com.iflytek.edcc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/17
  * Time: 17:13
  * Description
  */

object SparkCoreTest {

  //最佳位置。数据在哪台机器上，任务就启在哪个机器上，数据在本地上，(代码跟着数据走)
  // 不用走网络。不过数据进行最后汇总的时候就要走网络
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[4]")

    //spark程序入口，负责与集群交互
    val sc = new SparkContext(conf)

    //rdd的一个partition启动一个task
    //一个partition包含一个或多个blcok
    //一个hdfs的split正常对应一个partition
    val rdd = sc.parallelize(List((1,2),(1,4),(5,6),(5,4)),2)
    val rdd2 = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\data\\text",2)
    println("rdd数据存储级别："+rdd.getStorageLevel)
    println("rdd检查点文件："+rdd.getCheckpointFile)
//    println("rdd分区数："+rdd.getNumPartitions)

    println("rdd2:分区："+rdd2.partitions)
    println("rdd2依赖："+rdd2.dependencies)

    val p0 = rdd2.partitions(0)
    println("rdd2:分区1："+p0.index+" - "+p0.hashCode())
    val p1 = rdd2.partitions(1)
    println("rdd2:分区2："+p1.index+" - "+p1.hashCode())

    println("rdd2数据存储级别："+rdd2.getStorageLevel)
    println("rdd2检查点文件："+rdd2.getCheckpointFile)
    println("rdd2分区数："+rdd2.getNumPartitions)

    val haoopRdd = rdd2.dependencies(0).rdd
    println("rdd2依赖的rdd: "+haoopRdd)
    println("rdd2:分区1数据位置："+haoopRdd.preferredLocations(haoopRdd.partitions(0)))
    println("rdd2:分区2数据位置："+haoopRdd.preferredLocations(haoopRdd.partitions(1)))

    //广播变量
    val values = List(1,2,3,4,5,6)
    val broadcastValue = sc.broadcast(values)

    //累加器变量
    val accum = sc.accumulator(0,"ztwu")

    rdd.reduceByKey((x,y)=>{
      val temp = broadcastValue.value.size
      println("broadcastValue: "+temp)
      println("x,y: "+x+"-"+y)
      x+y+temp
    }).foreach(println)

    rdd.map(x=>{
      accum += 1
    }).cache().collect()
    //rdd,action操作才触发操作，并且每次都会依据rdd依赖从新开始计算
    println(accum.value)

    sc.stop()

  }

}
