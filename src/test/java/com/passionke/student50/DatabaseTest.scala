package com.passionke.student50

import com.wacai.stanlee.ProfileUtils
import org.apache.spark.SparkPlanExecutor
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/7/16.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class DatabaseTest extends FunSuite {

  val sparkSession: SparkSession = Database.sparkSession()
  /**
    * 1. 查询" 01 "课程比" 02 "课程成绩高的学生的信息及课程分数
    *
    */
  test("prob 01") {
    val rst = sparkSession.sql(
      """
        |select *
        |from (
        | select sId, score
        | from score
        | where cId = '01'
        |) t1
        |join (
        | select sId, score
        | from score
        | where cId = '02'
        |) t2
        |on t1.sId = t2.sId
        |where t1.score > t2.score
      """.stripMargin)

    rst.show()

    val schema = rst.schema
    val list = SparkPlanExecutor.doExec(rst.queryExecution.sparkPlan)

    list.map(_.toSeq(schema)).foreach(itr => println(itr.toString))

    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      SparkPlanExecutor.doExec(rst.queryExecution.sparkPlan)
      val end = System.currentTimeMillis()
      end - start
    }, 1000000, 1)

  }

}
