package edu.cuhk.rain.partitioner

import edu.cuhk.rain.util.lowerBound
import org.apache.spark.Partitioner

/**
 * A grid partitioner, which uses a regular grid to partition coordinates.
 *
 * @param rows        Number of rows.
 * @param cols        Number of columns.
 * @param rowsPerPart Number of rows per partition, which may be less at the bottom edge.
 * @param colsPerPart Number of columns per partition, which may be less at the right edge.
 */
class GridPartitioner(
                       val n: Int,
                       val thresholds: Array[Int]) extends Partitioner {

  require(n > 0)

  override val numPartitions: Int = thresholds.length * thresholds.length

  /**
   * Returns the index of the partition the input coordinate belongs to.
   *
   * @param key The partition id i (calculated through this method for coordinate (i, j) in
   *            `simulateMultiply`, the coordinate (i, j) or a tuple (i, j, k), where k is
   *            the inner index used in multiplication. k is ignored in computing partitions.
   * @return The index of the partition, which the coordinate belongs to.
   */
  override def getPartition(key: Any): Int = {
    key match {
      case i: Int => i
      case (i: Int, j: Int) =>
        getPartitionId(i, j)
      case (i: Int, j: Int, _: Int) =>
        getPartitionId(i, j)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key.")
    }
  }

  private def getPartitionId(i: Int): Int = {
    lowerBound(thresholds, i)
  }

  private def getPartitionId(i: Int, j: Int): Int = {
    i / rowsPerPart + j / colsPerPart * rowPartitions
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.n == r.n) && (this.thresholds sameElements r.thresholds)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      n: java.lang.Integer,
      thresholds: Array[Int])
  }
}

object GridPartitioner {
  /** Creates a new [[GridPartitioner]] instance. */
  def apply(rows: Int, cols: Int, rowsPerPart: Int, colsPerPart: Int): GridPartitioner = {
    new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
  }

  /** Creates a new [[GridPartitioner]] instance with the input suggested number of partitions. */
  def apply(rows: Int, cols: Int, suggestedNumPartitions: Int): GridPartitioner = {
    require(suggestedNumPartitions > 0)
    val scale: Double = 1.0 / math.sqrt(suggestedNumPartitions)
    val rowsPerPart: Int = math.round(math.max(scale * rows, 1.0)).toInt
    val colsPerPart: Int = math.round(math.max(scale * cols, 1.0)).toInt
    new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
  }
}