package com.github.passionke

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.{Spark, StarryTaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by passionke on 2018/5/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class PhysicalLogcalPlanTest extends FunSuite {

  val spark: SparkSession = Spark.sparkSession

  spark.sparkContext.setLogLevel("WARN")

  test("physical plan") {
    import spark.implicits._
    val seq = Seq(Person(10, "allo"))
    val df = spark.createDataset(seq)
    val sparkPlan = df.queryExecution.sparkPlan
    val rdd = sparkPlan.execute()
    val rPartitions = rdd.partitions
    val rs = rdd.compute(rPartitions.head, new StarryTaskContext)
    val list = rs.toList
    list.foreach(println)
    println(rs)
  }

  test("physical plan bl") {
    import spark.implicits._
    val rand = Random.nextInt(100) + 50
    val seq = 1.to(rand).map(i => Person(i, "peter"))
    val df = spark.createDataset(seq)
    df.createOrReplaceTempView("aloha")
    val df1 = spark.sql("select count(1) as cnt from aloha where age > 50 group by name")
    val sparkPlan = df1.queryExecution.sparkPlan
    val rdd = sparkPlan.execute()
    val rPartitions = rdd.partitions
    val taskContext = new StarryTaskContext
    val rs = rdd.compute(rPartitions.head, taskContext)
    val dateTypes = df1.schema.map(f => f.dataType)
    val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
    assert(list.head.head.asInstanceOf[Long].toInt.equals(rand - 50))
    taskContext.removeContext()
  }

  test("physical plan muti") {
    ProfileUtils.profileMuti(() => {
      import spark.implicits._
      val rand = Random.nextInt(1000) + 51
      val seq = 1.to(rand).map(i => Person(i, "peter"))
      val df = spark.createDataset(seq)
      val uuid = RandomStringUtils.randomAlphabetic(32)
      df.createOrReplaceTempView(uuid)
      val df1 = spark.sql(s"select count(1) as cnt from $uuid where age > 50 group by name")
      val sparkPlan = df1.queryExecution.sparkPlan
      val start = System.currentTimeMillis()
      val rdd = sparkPlan.execute()
      val end = System.currentTimeMillis()
      val rPartitions = rdd.partitions
      val taskContext = new StarryTaskContext
      val rs = rdd.compute(rPartitions.head, taskContext)
      val dateTypes = df1.schema.map(f => f.dataType)
      val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
      assert(list.head.head.asInstanceOf[Long].toInt.equals(rand - 50))
      spark.catalog.dropTempView(uuid)
      taskContext.removeContext()
      end - start
    }, 1, 10)
  }

  test("physical plan muti filter") {
    ProfileUtils.profileMuti(() => {
      import spark.implicits._
      val rand = Random.nextInt(1000) + 51
      val seq = 1.to(rand).map(i => Person(i, "peter"))
      val df = spark.createDataset(seq)
      val start = System.currentTimeMillis()
      val uuid = RandomStringUtils.randomAlphabetic(32)
      df.createOrReplaceTempView(uuid)
      val df1 = spark.sql(s"select name from $uuid where age =50 ")
      val sparkPlan = df1.queryExecution.executedPlan
      val rdd = sparkPlan.execute()
      val rPartitions = rdd.partitions
      val taskContext = new StarryTaskContext
      val rs = rdd.compute(rPartitions.head, taskContext)
      val end = System.currentTimeMillis()
      val dateTypes = df1.schema.map(f => f.dataType)
      val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
      assert(list.head.head.asInstanceOf[UTF8String].toString.equals("peter"))
      spark.catalog.dropTempView(uuid)
      taskContext.removeContext()
      end - start
    }, 1, 10)
  }

  test("physical plan join ") {
    ProfileUtils.profileMuti(() => {
      import spark.implicits._
      val seq = 1.to(10).map(i => Person(i, "peter"))
      val seq1 = 1.to(6).map(i => Person(1, "ford"))
      val df2 = spark.createDataset(seq1)
      df2.show()
      val df = spark.createDataset(seq)
      df.show()
      val start = System.currentTimeMillis()
      val uuid = RandomStringUtils.randomAlphabetic(32)
      df.createOrReplaceTempView(uuid + "_1")
      df2.createOrReplaceTempView(uuid + "_2")
      val df1 = spark.sql(
        s"""
           |select ${uuid + "_1"}.age, count(${uuid + "_1"}.name) as cnt
           |from ${uuid + "_1"}
           |join ${uuid + "_2"}
           |on ${uuid + "_1"}.age = ${uuid + "_2"}.age
           |group by ${uuid + "_1"}.age
         """.stripMargin)
      val sparkPlan = df1.queryExecution.sparkPlan
      val taskContext = new StarryTaskContext
      val rdd = sparkPlan.execute()
      val rPartitions = rdd.partitions
      val rs = rdd.compute(rPartitions.head, taskContext)
      val end = System.currentTimeMillis()
      val dateTypes = df1.schema.map(f => f.dataType)
      val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
      println(list.mkString("\n"))
      assert(list.head.apply(1).asInstanceOf[Long].longValue() == 6L)
      spark.catalog.dropTempView(uuid + "_1")
      spark.catalog.dropTempView(uuid + "_2")
      taskContext.removeContext()
      end - start
    }, 1, 1)
  }

  test("physical plan shuller ") {
    ProfileUtils.profileMuti(() => {
      import spark.implicits._
      val seq = 1.to(3).map(i => Person(1, "peter"))
      val df = spark.createDataset(seq)
      df.show()
      val start = System.currentTimeMillis()
      val uuid = RandomStringUtils.randomAlphabetic(32)
      df.createOrReplaceTempView(uuid)
      val df1 = spark.sql(
        s"""
           |select a.age, count(a.name) as cnt
           |from $uuid as a
           |join $uuid as b
           |on a.age = b.age
           |group by a.age
         """.stripMargin)
      val sparkPlan = df1.queryExecution.sparkPlan
      val taskContext = new StarryTaskContext
      val rdd = sparkPlan.execute()
      val rPartitions = rdd.partitions
      val rs = rdd.compute(rPartitions.head, taskContext)
      val end = System.currentTimeMillis()
      val dateTypes = df1.schema.map(f => f.dataType)
      val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
      println(list.mkString("\n"))
      spark.catalog.dropTempView(uuid + "_1")
      spark.catalog.dropTempView(uuid + "_2")
      taskContext.removeContext()
      end - start
    }, 1, 1)
  }

  test("union ") {
    ProfileUtils.profileMuti(() => {
      import spark.implicits._
      val seq = 1.to(3).map(i => Person(i, "peter"))
      val df = spark.createDataset(seq)
      df.show()
      val seq1 = 1.to(2).map(i => Person(4, "s"))
      val sf1 = spark.createDataset(seq1)
      val uuid1 = RandomStringUtils.randomAlphabetic(32)
      sf1.createTempView(uuid1)
      val start = System.currentTimeMillis()
      val uuid = RandomStringUtils.randomAlphabetic(32)
      df.createOrReplaceTempView(uuid)
      sf1.createOrReplaceTempView(uuid1)
      val df1 = spark.sql(
        s"""
           |select max(age) as mRepeat from (
           |  select age from ${uuid}
           |  union all
           |  select age from ${uuid1}
           |) a
         """.stripMargin)
      val sparkPlan = df1.queryExecution.executedPlan
      val taskContext = new StarryTaskContext
      val rdd = sparkPlan.execute()
      val rPartitions = rdd.partitions
      val rs = rdd.compute(rPartitions.head, taskContext)
      val end = System.currentTimeMillis()
      val dateTypes = df1.schema.map(f => f.dataType)
      val list = rs.map(iRow => iRow.toSeq(dateTypes)).toList
      println(list.mkString("\n"))
      spark.catalog.dropTempView(uuid + "_1")
      spark.catalog.dropTempView(uuid + "_2")
      taskContext.removeContext()
      end - start
    }, 1, 1)
  }

}

case class Person(age: Int, name: String)

