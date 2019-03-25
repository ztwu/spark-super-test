package com.iflytek.edcc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/18
  * Time: 14:09
  * Description
  */

//datasets的api测试
object SparkDataSetsTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.iflytek.edcc.MyRegistrator")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //导入sqlcontext的隐式函数
    import sqlContext.implicits._

    val user = sqlContext.read.text("data/project/edu_edcc/ztwu2/spark-super-test/data/user")

    val userRdd = user.map(x=>{
      val line = x.getAs[String](0).split("\t")
      val provinceId = line(0).toString
      val cityId = line(1).toString
      val districtId = line(2).toString
      val schoolId = line(3).toString
      val userId = line(4).toString
      User(provinceId,cityId,districtId,schoolId,userId)
    })
    val userDataSets = sqlContext.createDataset(userRdd)

    val action = sqlContext.read.text("data/project/edu_edcc/ztwu2/spark-super-test/data/action")

    action.rdd.foreach(x=>println(x))

    println("测试数据--------------------------------------")
    val actionRdd = action.map(x=>{
      val line = x.getAs[String](0).split("\t")
      val userId = line(0).toString
      val actionId = line(1).toString
      val actionNum = line(2).toInt
//      Event(userId,actionId,actionNum)
      new Event2(userId,actionId,actionNum)
    })

    actionRdd.foreach(x=>println(x))

//    val actionDataSets = sqlContext.createDataset(actionRdd)

//    userDataSets.show()
//    actionDataSets.show()

//    userDataSets
//        .joinWith[Event](actionDataSets,$"userId"===$"actoruserId").show()

//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id"))
//      .groupBy("province_id").sum("action_num").sort($"sum(action_num)".asc)
//      .where("province_id = 'p0'").filter($"province_id"==="p0").show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id"))
//      .groupBy("province_id").agg(("action_num","sum"),("school_id","count")).show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") and userDataFrame("school_id")===actionDataFrame("user_id"),"left")
//      .show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") && userDataFrame("school_id")===actionDataFrame("user_id"),"left")
//      .show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
//      .withColumn("user_id_2",actionDataFrame("user_id")).show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
//      .explode("school_id","school_id_"){
//        school_id:String=>{school_id.split("")}
//      }.show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
//      .explode("school_id","school_id_"){
//        school_id:String=>{school_id.split("")}
//      }.select($"province_id",$"school_id").show()
//
//    userDataFrame
//      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
//      .explode("school_id","school_id_"){
//        school_id:String=>{school_id.split("")}
//      }.select("province_id","school_id").show()
//
//    actionDataFrame.selectExpr("user_id","action_num + 1").show()
//
//    //自定义函数参数可以用_占位
//    sqlContext.udf.register("udftest",udftest(_:String))
//    actionDataFrame.selectExpr("udftest(user_id)").show()

    //using window functions currently requires a HiveContext
    //    val window = Window.partitionBy("user_id").orderBy($"action_num".desc)
    //    actionDataFrame.withColumn("rank",rank().over(window)).show()

    sc.stop()

  }

  def udftest(p:String):String = {
    p+"_udftest"
  }

}

case class User
(
  provinceId:String,
  cityId:String,
  districtId:String,
  schoolId:String,
  userId:String
)

case class Event
(
   actoruserId:String,
   actionId:String,
   actionNum:Int
 )
