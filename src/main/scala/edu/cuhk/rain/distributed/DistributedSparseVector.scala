package edu.cuhk.rain.distributed

import edu.cuhk.rain.graphBLAS.Semiring
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

// row vector
class DistributedSparseVector(
                               val v: RDD[(Int, Double)],
                               val size: Int
                             ) extends Serializable {

  def partitionBy(partitioner: Partitioner): DistributedSparseVector = {
    new DistributedSparseVector(v.partitionBy(partitioner), size)
  }

  def multiply(m: DistributedSparseMatrix): DistributedSparseVector = {
    multiply(m, Semiring.semiringPlusMul)
  }

  def multiply(m: DistributedSparseMatrix, semiring: Semiring[Double, Double]): DistributedSparseVector = {
    m.partitioner match {
      case Some(partitioner) => multiply(m, partitioner, semiring)
      case None => multiply(m, defaultPartitioner(m.rows, v), semiring)
    }
  }

  def multiply(m: DistributedSparseMatrix, partitioner: Partitioner, semiring: Semiring[Double, Double]): DistributedSparseVector = {
    require(size == m.numRows())
    val res: RDD[(Int, Double)] = m.rows.join(v, partitioner).flatMap { case (_, (ov, s)) =>
      ov.multiply(s, semiring.mul_Op).mapActive { case (i, v) => (i, v) }
    }.reduceByKey((x0, x1) => semiring.add_op(x0, x1))

    new DistributedSparseVector(res, m.numCols())
  }

  def collect(): SparseVector = {
    val value: Array[(Int, Double)] = v.sortByKey().collect()
    SparseVector(size, value)
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


object DistributedSparseVector {
  def fromSparseVector(sparseVector: SparseVector, sc: SparkContext): DistributedSparseVector = {
    val value: RDD[(Int, Double)] = sc.makeRDD(sparseVector.mapActive((u, v) => (u, v)))
    new DistributedSparseVector(value, sparseVector.size)
  }
}