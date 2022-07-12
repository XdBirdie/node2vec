package edu.cuhk.rain.test

import edu.cuhk.rain.test
import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

class DistributedSparseMatrix(
                               var rows: RDD[(Int, test.SparseVector)],
                               private var nRows: Int,
                               private var nCols: Int) {
  lazy val partitioner: Option[Partitioner] = rows.partitioner

  def this(rows: RDD[(Int, test.SparseVector)]) = this(rows, 0, 0)

  def partitionBy(partitioner: Partitioner): DistributedSparseMatrix = {
    new DistributedSparseMatrix(rows.partitionBy(partitioner), numRows(), numCols())
  }

  def multiply(other: DistributedSparseMatrix): DistributedSparseMatrix = {
    multiply(other, defaultPartitioner(rows, other.rows))
  }

  def multiply(other: DistributedSparseMatrix, partitioner: Partitioner): DistributedSparseMatrix = {
    require(numCols() == other.numRows())
    // 左侧的数据由行形式转为列形式
    val colsRDD: RDD[(Int, test.SparseVector)] = rows.flatMap { case (row, vector) =>
      vector.mapActive { case (col, value) =>
        (col, (row, value))
      }
    }.groupByKey(partitioner).mapValues { col => {
      val seq: Seq[(Int, Double)] = col.toSeq
      SparseVector.sparse(seq.length, seq)
    }
    }
    // 计算乘法
    val res: RDD[(Int, test.SparseVector)] =
      other.rows.join(colsRDD, partitioner).flatMap { case (_, (right, left)) =>
        left.mapActive { case (i, rowV) => (i, right.multiply(rowV)) }
      }.reduceByKey(partitioner, _ add _)
    new DistributedSparseMatrix(res, nRows, other.nCols)
  }

  def cache(): this.type = {
    rows.cache()
    this
  }

  def unpersist(blocking: Boolean = true): this.type = {
    rows.unpersist(blocking)
    this
  }

  def collect(): (Array[(Int, test.SparseVector)], Int, Int) = {
    (rows.collect(), numRows(), numCols())
  }

  def numRows(): Int = {
    if (nRows <= 0) {
      nRows = rows.map(_._1).reduce(math.max)
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
