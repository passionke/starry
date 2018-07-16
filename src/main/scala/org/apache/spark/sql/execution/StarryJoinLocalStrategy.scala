package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, StarryHashJoinExec, StarryNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryJoinLocalStrategy(conf: SQLConf) extends Strategy {

  /**
    * Matches a plan whose output should be small enough to be used in broadcast join.
    */
  private def canRunInLocalMemory(plan: LogicalPlan) = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.getConfString("spark.sql.maxLocalMemoryJoin", "10485760").toLong
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }


  def decideBuildSide(joinType: JoinType, left: LogicalPlan, right: LogicalPlan) = {
    val buildLeft = canBuildLeft(joinType) && canRunInLocalMemory(left)
    val buildRight = canBuildRight(joinType) && canRunInLocalMemory(right)

    def smallerSide =
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft

    if (buildRight && buildLeft) {
      smallerSide
    } else if (buildRight) {
      BuildRight
    } else if (buildLeft) {
      BuildLeft
    } else {
      smallerSide
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
      val buildSide = decideBuildSide(joinType, left, right)
      Seq(StarryHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))

    // --- SortMergeJoin ------------------------------------------------------------

    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if RowOrdering.isOrderable(leftKeys) =>
      joins.SortMergeJoinExec(
        leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

    // --- Without joining keys ------------------------------------------------------------

    // Pick BroadcastNestedLoopJoin if one side could be broadcast
    case j@logical.Join(left, right, joinType, condition) =>
      val buildSide = decideBuildSide(joinType, left, right)
      StarryNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil
    // Pick CartesianProduct for InnerJoin
    case logical.Join(left, right, _: InnerLike, condition) =>
      joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil
    case _ => Nil
  }
}
