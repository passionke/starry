package com.wacai.stanlee

import java.util.concurrent.{Callable, Executors, Future}

import org.apache.spark.internal.Logging

/**
  * Created by passionke on 2018/5/21.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object ProfileUtils extends Logging {

  def profileMuti(f: () => Long, times: Int, threads: Int): Unit = {
    val executorService = Executors.newFixedThreadPool(threads)

    val b: IndexedSeq[Future[Long]] = 1.to(times).map(_ => {
      executorService.submit(new Callable[Long] {
        override def call(): Long = {
          try {
            val t = f.apply()
            t
          } catch {
            case e: Exception => {}
              log.error("opps", e)
              0
          }
        }
      })
    })
   val ts = b.map(task => task.get())
    println(" mean " + mean(ts))
    println(" variance " + variance(ts))
    println(" std dev " + stdDev(ts))
  }

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))
}
