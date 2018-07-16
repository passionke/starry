package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkPlanExecutor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryHashJoinExec(
                               leftKeys: Seq[Expression],
                               rightKeys: Seq[Expression],
                               joinType: JoinType,
                               buildSide: BuildSide,
                               condition: Option[Expression],
                               left: SparkPlan,
                               right: SparkPlan)
  extends BinaryExecNode with HashJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "avgHashProbe" -> SQLMetrics.createAverageMetric(sparkContext, "avg hash probe"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val avgHashProbe = longMetric("avgHashProbe")
    val rows = SparkPlanExecutor.doExec(buildPlan)
    val hashed = HashedRelation(rows.iterator, buildKeys, rows.length, null)
    streamedPlan.execute().mapPartitions { streamedIter =>
      join(streamedIter, hashed, numOutputRows, avgHashProbe)
    }
  }
}