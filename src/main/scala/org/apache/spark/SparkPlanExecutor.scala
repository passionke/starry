package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ReuseSubquery, SparkPlan}

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object SparkPlanExecutor {

  def exec(plan: SparkPlan, sparkSession: SparkSession) = {
    val newPlan = Seq(
      ReuseSubquery(sparkSession.sessionState.conf))
      .foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    doExec(newPlan)
  }

  def firstPartition(rdd: RDD[InternalRow]): Partition = {
    rdd.partitions.head
  }

  def doExec(sparkPlan: SparkPlan): List[InternalRow] = {
    val rdd = sparkPlan.execute().map(ite => ite.copy())
    val partition = firstPartition(rdd)
    rdd.compute(partition, new StarryTaskContext).toList
  }

}
