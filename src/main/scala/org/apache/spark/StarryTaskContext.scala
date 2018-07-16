package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, TaskMemoryManager}
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

/**
  * Created by passionke on 2018/5/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class StarryTaskContext extends TaskContext {

  val memoryManager: MemoryManager = SparkEnv.get.memoryManager

  private var complete = false

  override def isCompleted(): Boolean = complete

  override def isInterrupted(): Boolean = false

  override def isRunningLocally(): Boolean = true

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = {
    this
  }

  override def stageId(): Int = 0

  override def stageAttemptNumber(): Int = 0

  override def partitionId(): Int = 0

  override def attemptNumber(): Int = 0

  override def taskAttemptId(): Long = 0

  override def getLocalProperty(key: String): String = {
    key
  }

  override def taskMetrics(): TaskMetrics = new TaskMetrics

  override def getMetricsSources(sourceName: String): Seq[Source] = Seq.empty

  override private[spark] def killTaskIfInterrupted(): Unit = {}

  override private[spark] def getKillReason() = None

  override private[spark] def taskMemoryManager() = new TaskMemoryManager(memoryManager, 0)

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {

  }

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {

  }

  TaskContext.setTaskContext(this)

  def removeContext(): Unit = {
    TaskContext.unset()
  }
}
