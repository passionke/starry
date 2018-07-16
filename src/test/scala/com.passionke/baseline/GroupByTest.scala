package com.passionke.baseline

import org.apache.spark.sql.execution.LocalBasedStrategies
import org.apache.spark.{Spark, SparkPlanExecutor}
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/4.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class GroupByTest extends FunSuite{

  test("group by") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val times:Int = 1
    val dumys = 1.to(times).map(i => Dumy("a", i, "sf"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.doExec(sparkPlan)
      val end = System.currentTimeMillis()
      assert(list.head.getLong(1).equals(times.toLong))
      end - start
    }, 10000000, 1)
  }

  test("group by 10 threads") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val l = 3000
    val dumys = 1.to(l).map(i => Dumy("a", i, i.toString))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getLong(1).equals(l))
      val end = System.currentTimeMillis()
      end - start
    }, 100000, 1)
  }

  test("raw group by") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "ass"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = df.collect()
      assert(list.head.getLong(1).equals(2L))
      val end = System.currentTimeMillis()
      end - start
    }, 1000, 1)
  }

  test("raw group by 10 threads") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "ass"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = df.collect()
      assert(list.head.getLong(1).equals(2L))
      val end = System.currentTimeMillis()
      end - start
    }, 1000, 20)
  }

  test("raw group by without starry") {
    val sparkSession = Spark.sparkSession
    LocalBasedStrategies.unRegister(sparkSession)
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "ass"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = df.collect()
      assert(list.head.getLong(1).equals(2L))
      val end = System.currentTimeMillis()
      end - start
    }, 1000, 1)
  }

  test("raw group by without starry 10 threads ") {
    val sparkSession = Spark.sparkSession
    LocalBasedStrategies.unRegister(sparkSession)
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "ass"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = df.collect()
      assert(list.head.getLong(1).equals(2L))
      val end = System.currentTimeMillis()
      end - start
    }, 1000, 10)
  }

}
