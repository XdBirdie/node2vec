package edu.cuhk.rain.linalg

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import breeze.linalg.{DenseMatrix => BDM, Matrix => BM}
import org.apache.spark.mllib.linalg.distributed.DistributedMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkException}

/**
 * A grid partitioner, which uses a regular grid to partition coordinates.
 *
 * @param rows        Number of rows.
 * @param cols        Number of columns.
 * @param rowsPerPart Number of rows per partition, which may be less at the bottom edge.
 * @param colsPerPart Number of columns per partition, which may be less at the right edge.
 */
class GridPartitioner(
                       val rows: Int,
                       val cols: Int,
                       val rowsPerPart: Int,
                       val colsPerPart: Int) extends Partitioner {

  require(rows > 0)
  require(cols > 0)
  require(rowsPerPart > 0)
  require(colsPerPart > 0)

  override val numPartitions: Int = rowPartitions * colPartitions
  private val rowPartitions: Int = math.ceil(rows * 1.0 / rowsPerPart).toInt
  private val colPartitions: Int = math.ceil(cols * 1.0 / colsPerPart).toInt

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

  /** Partitions sub-matrices as blocks with neighboring sub-matrices. */
  private def getPartitionId(i: Int, j: Int): Int = {
    require(0 <= i && i < rows, s"Row index $i out of range [0, $rows).")
    require(0 <= j && j < cols, s"Column index $j out of range [0, $cols).")
    i / rowsPerPart + j / colsPerPart * rowPartitions
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.rows == r.rows) && (this.cols == r.cols) &&
          (this.rowsPerPart == r.rowsPerPart) && (this.colsPerPart == r.colsPerPart)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      rows: java.lang.Integer,
      cols: java.lang.Integer,
      rowsPerPart: java.lang.Integer,
      colsPerPart: java.lang.Integer)
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

/**
 * Represents a distributed matrix in blocks of local matrices.
 *
 * @param blocks       The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
 *                     form this distributed matrix. If multiple blocks with the same index exist, the
 *                     results for operations like add and multiply will be unpredictable.
 * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
 *                     rows are not required to have the given number of rows
 * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
 *                     columns are not required to have the given number of columns
 * @param nRows        Number of rows of this matrix. If the supplied value is less than or equal to zero,
 *                     the number of rows will be calculated when `numRows` is invoked.
 * @param nCols        Number of columns of this matrix. If the supplied value is less than or equal to
 *                     zero, the number of columns will be calculated when `numCols` is invoked.
 */

class BlockGraphMatrix(
                        val blocks: RDD[((Int, Int), Matrix)],
                        val rowsPerBlock: Int,
                        val colsPerBlock: Int,
                        private var nRows: Long,
                        private var nCols: Long) extends DistributedMatrix {

  private type MatrixBlock = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), sub-matrix)
  /** Block (i,j) --> Set of destination partitions */
  private type BlockDestinations = Map[(Int, Int), Set[Int]]
  private lazy val blockInfo: RDD[((Int, Int), (Int, Int))] =
    blocks.mapValues(block => (block.numRows, block.numCols)).cache()
  val numRowBlocks: Int = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt
  val numColBlocks: Int = math.ceil(numCols() * 1.0 / colsPerBlock).toInt

  /**
   * Alternate constructor for BlockGraphMatrix without the input of the number of rows and columns.
   *
   * @param blocks       The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
   *                     form this distributed matrix. If multiple blocks with the same index exist, the
   *                     results for operations like add and multiply will be unpredictable.
   * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
   *                     rows are not required to have the given number of rows
   * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
   *                     columns are not required to have the given number of columns
   */
  def this(
            blocks: RDD[((Int, Int), Matrix)],
            rowsPerBlock: Int,
            colsPerBlock: Int) = {
    this(blocks, rowsPerBlock, colsPerBlock, 0L, 0L)
  }

  /**
   * Validates the block matrix info against the matrix data (`blocks`) and throws an exception if
   * any error is found.
   */
  def validate(): Unit = {
    // check if the matrix is larger than the claimed dimensions
    estimateDim()

    // Check if there are multiple MatrixBlocks with the same index.
    blockInfo.countByKey().foreach { case (key, cnt) =>
      if (cnt > 1) {
        throw new SparkException(s"Found multiple MatrixBlocks with the indices $key. Please " +
          "remove blocks with duplicate indices.")
      }
    }
    // Check if each MatrixBlock (except edges) has the dimensions rowsPerBlock x colsPerBlock
    // The first tuple is the index and the second tuple is the dimensions of the MatrixBlock
    val dimensionMsg: String = s"dimensions different than rowsPerBlock: $rowsPerBlock, and " +
      s"colsPerBlock: $colsPerBlock. Blocks on the right and bottom edges can have smaller " +
      s"dimensions. You may use the repartition method to fix this issue."
    blockInfo.foreach { case ((blockRowIndex, blockColIndex), (m, n)) =>
      if ((blockRowIndex < numRowBlocks - 1 && m != rowsPerBlock) ||
        (blockRowIndex == numRowBlocks - 1 && (m <= 0 || m > rowsPerBlock))) {
        throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
          dimensionMsg)
      }
      if ((blockColIndex < numColBlocks - 1 && n != colsPerBlock) ||
        (blockColIndex == numColBlocks - 1 && (n <= 0 || n > colsPerBlock))) {
        throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
          dimensionMsg)
      }
    }
  }

  /** Estimates the dimensions of the matrix. */
  private def estimateDim(): Unit = {
    val (rows, cols) = blockInfo.map { case ((blockRowIndex, blockColIndex), (m, n)) =>
      (blockRowIndex.toLong * rowsPerBlock + m,
        blockColIndex.toLong * colsPerBlock + n)
    }.reduce { (x0, x1) =>
      (math.max(x0._1, x1._1), math.max(x0._2, x1._2))
    }
    if (nRows <= 0L) nRows = rows
    assert(rows <= nRows, s"The number of rows $rows is more than claimed $nRows.")
    if (nCols <= 0L) nCols = cols
    assert(cols <= nCols, s"The number of columns $cols is more than claimed $nCols.")
  }

  /** Caches the underlying RDD. */
  def cache(): this.type = {
    blocks.cache()
    this
  }

  /** Persists the underlying RDD with the specified storage level. */
  def persist(storageLevel: StorageLevel): this.type = {
    blocks.persist(storageLevel)
    this
  }

  /**
   * Transpose this `BlockGraphMatrix`. Returns a new `BlockGraphMatrix` instance sharing the
   * same underlying data. Is a lazy operation.
   */
  def transpose: BlockGraphMatrix = {
    val transposedBlocks: RDD[((Int, Int), Matrix)] =
      blocks.map { case ((blockRowIndex, blockColIndex), mat) =>
        ((blockColIndex, blockRowIndex), mat.transpose)
      }
    new BlockGraphMatrix(transposedBlocks, colsPerBlock, rowsPerBlock, nCols, nRows)
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  def toBreeze(): BDM[Double] = {
    val localMat: Matrix = toLocalMatrix()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
  }

  /**
   * Collect the distributed matrix on the driver as a `DenseMatrix`.
   */
  def toLocalMatrix(): Matrix = {
    require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
      s"Int.MaxValue. Currently numRows: ${numRows()}")
    require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
      s"Int.MaxValue. Currently numCols: ${numCols()}")
    require(numRows() * numCols() < Int.MaxValue, "The length of the values array must be " +
      s"less than Int.MaxValue. Currently numRows * numCols: ${numRows() * numCols()}")
    val m: Int = numRows().toInt
    val n: Int = numCols().toInt
    //    val mem: Int = m * n / 125000
    val localBlocks: Array[((Int, Int), Matrix)] = blocks.collect()
    val values = new Array[Double](m * n)
    localBlocks.foreach { case ((blockRowIndex, blockColIndex), submat) =>
      val rowOffset: Int = blockRowIndex * rowsPerBlock
      val colOffset: Int = blockColIndex * colsPerBlock
      submat.foreachActive { (i, j, v) =>
        val indexOffset: Int = (j + colOffset) * m + rowOffset + i
        values(indexOffset) = v
      }
    }
    new DenseMatrix(m, n, values)
  }

  /**
   * Adds the given block matrix `other` to `this` block matrix: `this + other`.
   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
   * values. If one of the blocks that are being added are instances of `SparseMatrix`,
   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being added
   * to a `DenseMatrix`. If two dense matrices are added, the output will also be a
   * `DenseMatrix`.
   */
  def add(other: BlockGraphMatrix): BlockGraphMatrix =
    blockMap(other, (x: BM[Double], y: BM[Double]) => x + y)

  /**
   * Subtracts the given block matrix `other` from `this` block matrix: `this - other`.
   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
   * values. If one of the blocks that are being subtracted are instances of `SparseMatrix`,
   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being subtracted
   * from a `DenseMatrix`. If two dense matrices are subtracted, the output will also be a
   * `DenseMatrix`.
   */
  def subtract(other: BlockGraphMatrix): BlockGraphMatrix =
    blockMap(other, (x: BM[Double], y: BM[Double]) => x - y)

  /**
   * For given matrices `this` and `other` of compatible dimensions and compatible block dimensions,
   * it applies a binary function on their corresponding blocks.
   *
   * @param other  The second BlockGraphMatrix argument for the operator specified by `binMap`
   * @param binMap A function taking two breeze matrices and returning a breeze matrix
   * @return A [[BlockGraphMatrix]] whose blocks are the results of a specified binary map on blocks
   *         of `this` and `other`.
   *         Note: `blockMap` ONLY works for `add` and `subtract` methods and it does not support
   *         operators such as (a, b) => -a + b
   *         TODO: Make the use of zero matrices more storage efficient.
   */
  def blockMap(
                other: BlockGraphMatrix,
                binMap: (BM[Double], BM[Double]) => BM[Double]): BlockGraphMatrix = {
    require(numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
      s"A.numRows: ${numRows()}, B.numRows: ${other.numRows()}")
    require(numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
      s"A.numCols: ${numCols()}, B.numCols: ${other.numCols()}")
    if (rowsPerBlock == other.rowsPerBlock && colsPerBlock == other.colsPerBlock) {
      val newBlocks: RDD[((Int, Int), Matrix)] = blocks.cogroup(other.blocks, createPartitioner())
        .map { case ((blockRowIndex, blockColIndex), (a, b)) =>
          if (a.size > 1 || b.size > 1) {
            throw new SparkException("There are multiple MatrixBlocks with indices: " +
              s"($blockRowIndex, $blockColIndex). Please remove them.")
          }
          if (a.isEmpty) {
            val zeroBlock: BM[Double] = BM.zeros[Double](b.head.numRows, b.head.numCols)
            val result: BM[Double] = binMap(zeroBlock, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
          } else if (b.isEmpty) {
            new MatrixBlock((blockRowIndex, blockColIndex), a.head)
          } else {
            val result: BM[Double] = binMap(a.head.asBreeze, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
          }
        }
      new BlockGraphMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
    } else {
      throw new SparkException("Cannot perform on matrices with different block dimensions")
    }
  }

  def createPartitioner(): GridPartitioner =
    GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.partitions.length)

  def multiply(
                other: BlockGraphMatrix,
                mul: (Matrix, Matrix) => Matrix,
                add: (Matrix, Matrix) => Matrix
              ): Unit = {
    val resultPartitioner: GridPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
      math.max(blocks.partitions.length, other.blocks.partitions.length))
    val (leftDestinations, rightDestinations) = simulateMultiply(other, resultPartitioner, 1)
    // Each block of A must be multiplied with the corresponding blocks in the columns of B.
    val flatA: RDD[(Int, (Int, Int, Matrix))] = blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
      val destinations: Set[Int] = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
      destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
    }
    // Each block of B must be multiplied with the corresponding blocks in each row of A.
    val flatB: RDD[(Int, (Int, Int, Matrix))] = other.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
      val destinations: Set[Int] = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
      destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
    }
    val intermediatePartitioner: Partitioner = new Partitioner {
      override def numPartitions: Int = resultPartitioner.numPartitions * 1

      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }
    val newBlocks: RDD[((Int, Int), Matrix)] = flatA.cogroup(flatB, intermediatePartitioner).flatMap { case (_, (a, b)) =>
      a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
        b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
          ((leftRowIndex, rightColIndex), mul(leftBlock, rightBlock))
        }
      }
    }.reduceByKey(resultPartitioner, (a, b) => add(a, b))
    // TODO: Try to use aggregateByKey instead of reduceByKey to get rid of intermediate matrices
    new BlockGraphMatrix(newBlocks, rowsPerBlock, other.colsPerBlock, numRows(), other.numCols())
  }

  override def numRows(): Long = {
    if (nRows <= 0L) estimateDim()
    nRows
  }

  override def numCols(): Long = {
    if (nCols <= 0L) estimateDim()
    nCols
  }

  /**
   * Simulate the multiplication with just block indices in order to cut costs on communication,
   * when we are actually shuffling the matrices.
   * The `colsPerBlock` of this matrix must equal the `rowsPerBlock` of `other`.
   * Exposed for tests.
   *
   * @param other       The BlockGraphMatrix to multiply
   * @param partitioner The partitioner that will be used for the resulting matrix `C = A * B`
   * @return A tuple of [[BlockDestinations]]. The first element is the Map of the set of partitions
   *         that we need to shuffle each blocks of `this`, and the second element is the Map for
   *         `other`.
   */
  def simulateMultiply(
                        other: BlockGraphMatrix,
                        partitioner: GridPartitioner,
                        midDimSplitNum: Int): (BlockDestinations, BlockDestinations) = {
    val leftMatrix: Array[(Int, Int)] = blockInfo.keys.collect()
    val rightMatrix: Array[(Int, Int)] = other.blockInfo.keys.collect()

    val rightCounterpartsHelper: Map[Int, Array[Int]] = rightMatrix.groupBy(_._1).mapValues(_.map(_._2))
    val leftDestinations: Map[(Int, Int), Set[Int]] = leftMatrix.map { case (rowIndex, colIndex) =>
      val rightCounterparts: Array[Int] = rightCounterpartsHelper.getOrElse(colIndex, Array.empty[Int])
      val partitions: Array[Int] = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b)))
      val midDimSplitIndex: Int = colIndex % midDimSplitNum
      ((rowIndex, colIndex),
        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
    }.toMap

    val leftCounterpartsHelper: Map[Int, Array[Int]] = leftMatrix.groupBy(_._2).mapValues(_.map(_._1))
    val rightDestinations: Map[(Int, Int), Set[Int]] = rightMatrix.map { case (rowIndex, colIndex) =>
      val leftCounterparts: Array[Int] = leftCounterpartsHelper.getOrElse(rowIndex, Array.empty[Int])
      val partitions: Array[Int] = leftCounterparts.map(b => partitioner.getPartition((b, colIndex)))
      val midDimSplitIndex = rowIndex % midDimSplitNum
      ((rowIndex, colIndex),
        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
    }.toMap

    (leftDestinations, rightDestinations)
  }
}
