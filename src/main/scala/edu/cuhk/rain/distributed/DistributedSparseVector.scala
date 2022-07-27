package edu.cuhk.rain.distributed

import edu.cuhk.rain.graphBLAS.{Monoid, Semiring}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.broadcast.Broadcast
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

  private def coPartitioner(other: DistributedSparseVector): Partitioner = {
    v.partitioner match {
      case Some(partitioner) => partitioner
      case None => defaultPartitioner(v, other.v)
    }
  }

  def coOperate(other: DistributedSparseVector, monoid: Monoid[Double]):DistributedSparseVector = {
    require(this.size == other.size)

    val bcMonoid: Broadcast[Monoid[Double]] = v.context.broadcast(monoid)
    val res: RDD[(Int, Double)] = v.fullOuterJoin(
      other.v, coPartitioner(other)
    ).mapValues { case (x, y) =>
      val i: Double = bcMonoid.value.identity
      bcMonoid.value(x.getOrElse(i), y.getOrElse(i))
    }
    new DistributedSparseVector(res, size)
  }

  def add(other: DistributedSparseVector): DistributedSparseVector = {
    require(this.size == other.size)

    val res: RDD[(Int, Double)] = v.fullOuterJoin(
      other.v, coPartitioner(other)
    ).mapValues { case (x, y) =>
      x.getOrElse(0.0) + y.getOrElse(0.0)
    }
    new DistributedSparseVector(res, size)
  }

  def multiply(other: DistributedSparseVector): DistributedSparseVector = {
    require(this.size == other.size)

    val res: RDD[(Int, Double)] = v.fullOuterJoin(
      other.v, coPartitioner(other)
    ).mapValues { case (x, y) =>
      x.getOrElse(0.0) * y.getOrElse(0.0)
    }
    new DistributedSparseVector(res, size)
  }

  def div(other: DistributedSparseVector): DistributedSparseVector = {
    require(this.size == other.size)

    val res: RDD[(Int, Double)] = v.fullOuterJoin(
      other.v, coPartitioner(other)
    ).mapValues { case (x, y) =>
      if (x.isEmpty || y.isEmpty) 0.0
      else x.get / y.get
    }
    new DistributedSparseVector(res, size)
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
    require(size == m.numRows(), s"size: $size, m.numRows: ${m.numRows()}")
    val res: RDD[(Int, Double)] = m.rows.join(v, partitioner).flatMap { case (_, (ov, s)) =>
      ov.multiply(s, semiring.mul_Op).values
    }.reduceByKey(partitioner, (x0, x1) => semiring.add_op(x0, x1))

    new DistributedSparseVector(res, m.numCols())
  }

  def maskBy(other: DistributedSparseVector, positive: Boolean=true): DistributedSparseVector = {
    val bcPositive: Broadcast[Boolean] = v.context.broadcast(positive)

    val res: RDD[(Int, Double)] = v.fullOuterJoin(
      other.v, coPartitioner(other)
    ).mapValues { case (x, mask) =>
      if (mask.isEmpty ^ bcPositive.value) None
      else x
    }.filter(_._2.isDefined).mapValues(_.get)

    bcPositive.unpersist(false)
    new DistributedSparseVector(res, size)
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
    val value: RDD[(Int, Double)] = sc.makeRDD(sparseVector.values)
    new DistributedSparseVector(value, sparseVector.size)
  }

  def random(size: Int, sc: SparkContext): DistributedSparseVector = {
    val values: RDD[(Int, Double)] =
      sc.makeRDD(Array.range(0, size) zip Array.fill(size)(math.random))
    new DistributedSparseVector(values, size)
  }
}