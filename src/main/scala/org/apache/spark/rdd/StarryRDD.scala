package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class StarryRDD[T: ClassTag](sc: SparkContext,
                             rddName: String,
                             @transient private var data: Seq[T]
                            ) extends RDD[T](sc, Nil) {
  def this (sc: SparkContext, data: Seq[T]) = {
    this (sc, getClass.getSimpleName, data)
  }

  setName(rddName)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

  def updateData(data: Seq[T]): Unit = {
    this.data = data
    this.markCheckpointed()
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new ParallelCollectionPartition(id, 0, data))
  }
}
