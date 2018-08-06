package com.github.passionke.baseline

import com.github.passionke.ProfileUtils
import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.Spark
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/2.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class JoinSqlTest extends FunSuite {

  test("join") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select a1.name, count(1) as cnt
        |from a as a1 join a as a2
        |on a1.name = a2.name
        |group by a1.name
      """.stripMargin)

    df.show()
    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getLong(1).equals(1L))
      val end = System.currentTimeMillis()
      end - start
    }, 10000000, 1)
  }

  test("join 1") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("b", 20, "bbb"))
    dumys.toDF().createOrReplaceTempView("a")

    val dumys1 = Seq(Dumy("b", 10, "abc"), Dumy("b", 20, "bbb"))
    dumys1.toDF().createOrReplaceTempView("b")
    val df = sparkSession.sql(
      """
        |select a1.name, count(1) as cnt
        |from a as a1 join b as a2
        |on a1.name = a2.name
        |group by a1.name
      """.stripMargin)

    df.show()
    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getLong(1).equals(2L))
      val end = System.currentTimeMillis()
      end - start
    }, 10000000, 1)
  }

}
