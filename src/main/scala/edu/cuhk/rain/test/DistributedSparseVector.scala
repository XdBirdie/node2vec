package edu.cuhk.rain.test

import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

// row vector
class DistributedSparseVector(
                               val v: RDD[(Int, Double)],
                               val size: Int
                             ) extends Serializable {

  def partitionBy(partitioner: Partitioner): DistributedSparseVector = {
    new DistributedSparseVector(v.partitionBy(partitioner), size)
  }

  def multiply(m: DistributedSparseMatrix): DistributedSparseVector = {
    m.partitioner match {
      case Some(partitioner) => multiply(m, partitioner)
      case None => multiply(m, defaultPartitioner(m.rows, v))
    }
  }

  def multiply(m: DistributedSparseMatrix, partitioner: Partitioner): DistributedSparseVector = {
    require(size == m.numRows())
    val res: RDD[(Int, Double)] = m.rows.join(v, partitioner).flatMap { case (_, (ov, s)) =>
      ov.multiply(s).mapActive { case (i, v) => (i, v) }
    }.reduceByKey(_ + _)

    new DistributedSparseVector(res, m.numCols())
  }

  def cache(): this.type = {
    v.cache()
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    v.unpersist(blocking)
    this
  }
}
