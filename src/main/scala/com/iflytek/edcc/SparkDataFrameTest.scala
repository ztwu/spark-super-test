package com.iflytek.edcc

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//使用窗口函数
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/18
  * Time: 14:09
  * Description
  */

//dataframe的api测试
object SparkDataFrameTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //导入sqlcontext的隐式函数
    import sqlContext.implicits._

    val user = sqlContext.read.text("D:\\project\\edu_edcc\\ztwu2\\spark-super-test\\data\\user")

    val userDataFrame = user.map(x=>{
      val line = x.getAs[String](0).split("\t")
      val provinceId = line(0)
      val cityId = line(1)
      val districtId = line(2)
      val schoolId = line(3).toString
      val userId = line(4)
      (provinceId,cityId,districtId,schoolId,userId)
    }).toDF("province_id","city_id","district_id","school_id","user_id")

    val action = sqlContext.read.text("D:\\project\\edu_edcc\\ztwu2\\spark-super-test\\data\\action")
    val actionDataFrame = action.map(x=>{
      val line = x.getAs[String](0).split("\t")
      val userId = line(0)
      val actionId = line(1)
      val actionNum = line(2).toInt
      (userId,actionId,actionNum)
    }).toDF("user_id","action_id","action_num")

    userDataFrame.show()
    actionDataFrame.show()

    userDataFrame.cache()
    actionDataFrame.cache()

    userDataFrame.
      join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id"),"inner")
      .groupBy("province_id").count().sort($"count".asc).show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id"))
      .groupBy("province_id").sum("action_num").sort($"sum(action_num)".asc)
        .where("province_id = 'p0'").filter($"province_id"==="p0").show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id"))
      .groupBy("province_id").agg(("action_num","sum"),("school_id","count")).show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") and userDataFrame("school_id")===actionDataFrame("user_id"),"left")
      .show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") && userDataFrame("school_id")===actionDataFrame("user_id"),"left")
      .show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
        .withColumn("user_id_2",actionDataFrame("user_id")).show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
      .explode("school_id","school_id_"){
        school_id:String=>{school_id.split("")}
      }.show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
      .explode("school_id","school_id_"){
        school_id:String=>{school_id.split("")}
      }.select($"province_id",$"school_id").show()

    userDataFrame
      .join(actionDataFrame,userDataFrame("user_id")===actionDataFrame("user_id") ,"left")
      .explode("school_id","school_id_"){
        school_id:String=>{school_id.split("")}
      }.select("province_id","school_id").show()

    actionDataFrame.selectExpr("user_id","action_num + 1").show()

    //自定义函数参数可以用_占位
    sqlContext.udf.register("udftest",udftest(_:String))
    actionDataFrame.selectExpr("udftest(user_id)").show()

    //using window functions currently requires a HiveContext
//    val window = Window.partitionBy("user_id").orderBy($"action_num".desc)
//    actionDataFrame.withColumn("rank",rank().over(window)).show()

    sc.stop()

  }

  def udftest(p:String):String = {
    p+"_udftest"
  }

}
