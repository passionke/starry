package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class StarryRDD[T: ClassTag](sc: SparkContext,
                             @transient private val data: Seq[T]
                            ) extends RDD[T](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new ParallelCollectionPartition(id, 0, data))
  }
}
