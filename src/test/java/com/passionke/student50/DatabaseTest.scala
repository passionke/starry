package com.passionke.student50

import com.wacai.stanlee.ProfileUtils
import org.apache.spark.rdd.{RDD, StarryRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{Dependency, SparkPlanExecutor}
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

    val rootPlans = rst.queryExecution.sparkPlan.find(plan => {
      plan.children.isEmpty
    })
    list.map(_.toSeq(schema)).foreach(itr => println(itr.toString))

    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      SparkPlanExecutor.doExec(rst.queryExecution.sparkPlan)
      val end = System.currentTimeMillis()
      end - start
    }, 10000, 1)
  }

  test("prob 01_rdd") {
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
    val rdd = rst.queryExecution.sparkPlan.execute().map(_.copy())
    list.map(_.toSeq(schema)).foreach(itr => println(itr.toString))

    rdd.dependencies.flatMap(r => r.rdd.dependencies)
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      SparkPlanExecutor.rddCompute(rdd)
      val end = System.currentTimeMillis()
      end - start
    }, 10000, 1)
  }

  test("prob 01_rdd change data") {
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
    val rdd = rst.queryExecution.sparkPlan.execute().map(_.copy())
    list.map(_.toSeq(schema)).foreach(itr => println(itr.toString))

    val rootRdd = getRootRdd(rdd)

    val d1 = sparkSession.createDataFrame(Score.scores().filterNot(s => s.sId.equals("02") && s.score.equals(70L)))

    val d2 = d1.collect().map(row => RowEncoder.apply(d1.schema).toRow(row))
    val rootStarryRdd = rootRdd.head.asInstanceOf[StarryRDD[InternalRow]]
    rootStarryRdd.updateData(d2)
    rdd.dependencies.flatMap(r => r.rdd.dependencies)
    val start = System.currentTimeMillis()
    val l2 = SparkPlanExecutor.rddCompute(rdd)
    println("after rdd update")
    assert(l2.lengthCompare(1) == 0)
    l2.map(_.toSeq(schema)).foreach(itr => println(itr.toString()))
    val end = System.currentTimeMillis()
    end - start
  }

  test("prob 01_rdd change data perf") {
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
    val rdd = rst.queryExecution.sparkPlan.execute().map(_.copy())
    list.map(_.toSeq(schema)).foreach(itr => println(itr.toString))

    val rootRdd = getRootRdd(rdd)

    val d1 = sparkSession.createDataFrame(Score.scores().filterNot(s => s.sId.equals("02") && s.score.equals(70L)))

    val d2 = d1.collect().map(row => RowEncoder.apply(d1.schema).toRow(row))
    val rootStarryRdd = rootRdd.head.asInstanceOf[StarryRDD[InternalRow]]
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      rootStarryRdd.updateData(d2)
      SparkPlanExecutor.rddCompute(rdd)
      val end = System.currentTimeMillis()
      end - start
    }, 10000, 1)
    val l2 = SparkPlanExecutor.rddCompute(rdd)
    assert(l2.lengthCompare(1) == 0)
    l2.map(_.toSeq(schema)).foreach(itr => println(itr.toString()))
  }

  def getRootRdd(rdd: RDD[InternalRow]): Seq[RDD[InternalRow]] = {
    if (rdd.dependencies.isEmpty) {
      Seq(rdd)
    } else {
      rdd.dependencies.flatMap(dep => getRootRdd(dep.asInstanceOf[Dependency[InternalRow]].rdd))
    }
  }
}
