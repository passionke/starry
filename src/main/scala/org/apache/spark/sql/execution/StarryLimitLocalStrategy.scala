package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.exchange.StarryTakeOrderedAndProjectExec

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryLimitLocalStrategy() extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(rootPlan) => rootPlan match {
      case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
        StarryTakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
        StarryTakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), child) =>
        // With whole stage codegen, Spark releases resources only when all the output data of the
        // query plan are consumed. It's possible that `CollectLimitExec` only consumes a little
        // data from child plan and finishes the query without releasing resources. Here we wrap
        // the child plan with `LocalLimitExec`, to stop the processing of whole stage codegen and
        // trigger the resource releasing work, after we consume `limit` rows.
        StarryCollectLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
      case other => planLater(other) :: Nil
    }
    case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
      StarryTakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
    case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
      StarryTakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
    case _ => Nil
  }
}
