package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.stanlee.execution.aggregate.StarryAggUtils

/**
  * Created by passionke on 2018/6/8.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryAggStrategy() extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalAggregation(
    groupingExpressions, aggregateExpressions, resultExpressions, child) =>

      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggregateExpressions.partition(_.isDistinct)
      if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
        // This is a sanity check. We should not reach here when we have multiple distinct
        // column sets. Our MultipleDistinctRewriter should take care this case.
        sys.error("You hit a query analyzer bug. Please report your query to " +
          "Spark user mailing list.")
      }

      val aggregateOperator =
        if (functionsWithDistinct.isEmpty) {
          StarryAggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            aggregateExpressions,
            resultExpressions,
            planLater(child))
        } else {
          StarryAggUtils.planAggregateWithOneDistinct(
            groupingExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(child))
        }

      aggregateOperator

    case _ => Nil
  }
}
