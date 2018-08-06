package com.github.passionke.student50

import org.apache.spark.Spark
import org.apache.spark.sql.SparkSession

/**
  * Created by passionke on 2018/7/16.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object Database {

  def sparkSession(): SparkSession = {
    val sparkSession: SparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession.createDataFrame(Student.students()).createOrReplaceTempView("student")
    sparkSession.createDataFrame(Teacher.teachers()).createOrReplaceTempView("techer")
    sparkSession.createDataFrame(Score.scores()).createOrReplaceTempView("score")
    sparkSession.createDataFrame(Course.courses()).createOrReplaceTempView("course")
    sparkSession
  }
}
