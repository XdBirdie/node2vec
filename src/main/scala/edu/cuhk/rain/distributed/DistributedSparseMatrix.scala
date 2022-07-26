package edu.cuhk.rain.distributed

import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class DistributedSparseMatrix(
                               var rows: RDD[(Int, SparseVector)],
                               private var nRows: Int,
                               private var nCols: Int) {
  lazy val partitioner: Option[Partitioner] = rows.partitioner

  def this(rows: RDD[(Int, SparseVector)]) = this(rows, 0, 0)

  def partitionBy(partitioner: Partitioner): DistributedSparseMatrix = {
    new DistributedSparseMatrix(rows.partitionBy(partitioner), numRows(), numCols())
  }

  def multiply(other: DistributedSparseMatrix): DistributedSparseMatrix = {
    multiply(other, defaultPartitioner(rows, other.rows))
  }

  def multiply(other: DistributedSparseMatrix, partitioner: Partitioner): DistributedSparseMatrix = {
    require(numCols() == other.numRows())
    // 左侧的数据由行形式转为列形式
    val bcNumCols: Broadcast[Int] = rows.context.broadcast(numCols())
    val colsRDD: RDD[(Int, SparseVector)] = rows.flatMap { case (row, vector) =>
      vector.mapActive { case (col, value) =>
        (col, (row, value))
      }
    }.groupByKey(partitioner).mapValues { it =>
      val values: Array[(Int, Double)] = it.toArray
      new SparseVector(bcNumCols.value, values)
    }
    // 计算乘法
    val res: RDD[(Int, SparseVector)] =
      other.rows.join(colsRDD, partitioner).flatMap { case (_, (right, left)) =>
        left.mapActive { case (i, rowV) => (i, right.multiply(rowV)) }
      }.reduceByKey(partitioner, _ add _)
    new DistributedSparseMatrix(res, numRows(), other.numCols())
  }

  def count(): Long = rows.count()

  def cache(): this.type = {
    rows.cache()
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    rows.unpersist(blocking)
    this
  }

  def collect(): (Array[(Int, SparseVector)], Int, Int) = {
    (rows.collect(), numRows(), numCols())
  }

  def numRows(): Int = {
    if (nRows <= 0) {
      nRows = rows.map(_._1).reduce(math.max) + 1
    }
    nRows
  }

  def numCols(): Int = {
    if (nCols <= 0L) {
      nCols = rows.first()._2.size
    }
    nCols
  }
}

object DistributedSparseMatrix {
  def fromEdgeList(
                    edges: RDD[(Int, Int)],
                    size: Int,
                    directed: Boolean,
                    ids: Boolean=false
                  ): DistributedSparseMatrix = {
    val f: (Int, Int) => Array[(Int, Int)] =
      if (directed) (u: Int, v: Int) => Array((u, v))
      else (u: Int, v: Int) => Array((u, v), (v, u))
    val value: RDD[(Int, Int)] =
      edges.mapPartitions(it => it.flatMap { case (u, v) => f(u, v) })

    val rows: RDD[(Int, SparseVector)] = {
      if (ids) value.groupByKey().map{ case (u, it) =>
        val indices: Array[Int] = it.toArray
        (u, new SparseVector(size, indices zip Array.fill(indices.length)(u.toDouble)))
      } else value.groupByKey().mapValues(it => SparseVector.ones(size, it.toArray))
    }
    new DistributedSparseMatrix(rows, size, size)
  }
}