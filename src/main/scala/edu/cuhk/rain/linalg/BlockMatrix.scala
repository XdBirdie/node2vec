//package edu.cuhk.rain.linalg
//
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import breeze.linalg.{DenseMatrix => BDM, Matrix => BM}
//import edu.cuhk.rain.partitioner.GridPartitioner
//import org.apache.spark.mllib.linalg.distributed.DistributedMatrix
//import org.apache.spark.rdd.RDD
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.{Partitioner, SparkException}
//
///**
// * Represents a distributed matrix in blocks of local matrices.
// *
// * @param blocks       The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
// *                     form this distributed matrix. If multiple blocks with the same index exist, the
// *                     results for operations like add and multiply will be unpredictable.
// * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
// *                     rows are not required to have the given number of rows
// * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
// *                     columns are not required to have the given number of columns
// * @param nRows        Number of rows of this matrix. If the supplied value is less than or equal to zero,
// *                     the number of rows will be calculated when `numRows` is invoked.
// * @param nCols        Number of columns of this matrix. If the supplied value is less than or equal to
// *                     zero, the number of columns will be calculated when `numCols` is invoked.
// */
//
//class BlockGraphMatrix(
//                   val blocks: RDD[(Int, Matrix)],
//                   private var nRows: Long,
//                   private var nCols: Long) extends DistributedMatrix {
//
//  private type MatrixBlock = (Int, Matrix) // (blockRowIndex, sub-matrix)
//  /** Block (i,j) --> Set of destination partitions */
//  private type BlockDestinations = Map[(Int, Int), Set[Int]]
//  private lazy val blockInfo: Array[(Int, (Int, Int))] =
//    blocks.mapValues(block => (block.numRows, block.numCols)).collect()
////  val numRowBlocks: Int = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt
//
//  /**
//   * Alternate constructor for BlockGraphMatrix without the input of the number of rows and columns.
//   *
//   * @param blocks       The RDD of sub-matrix blocks ((blockRowIndex, blockColIndex), sub-matrix) that
//   *                     form this distributed matrix. If multiple blocks with the same index exist, the
//   *                     results for operations like add and multiply will be unpredictable.
//   * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
//   *                     rows are not required to have the given number of rows
//   * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
//   *                     columns are not required to have the given number of columns
//   */
//  def this(blocks: RDD[(Int, Matrix)]) = {
//    this(blocks, 0L, 0L)
//  }
//
//  /**
//   * Validates the block matrix info against the matrix data (`blocks`) and throws an exception if
//   * any error is found.
//   */
//  def validate(): Unit = {
//    // check if the matrix is larger than the claimed dimensions
//    estimateDim()
//    blockInfo.groupBy(_._1).foreach{case (key, it) =>
//      if (it.length > 1) {
//        throw new SparkException(s"Found multiple MatrixBlocks with the indices $key. Please " +
//          "remove blocks with duplicate indices.")
//      }
//    }
//
//    // Check if each MatrixBlock (except edges) has the dimensions rowsPerBlock x colsPerBlock
//    // The first tuple is the index and the second tuple is the dimensions of the MatrixBlock
//    val dimensionMsg: String = s" Blocks on the right and bottom edges can have smaller " +
//      s"dimensions. You may use the repartition method to fix this issue."
//    val numCol: Long = numCols()
//    var sumRows = 0
//    blockInfo.foreach { case (blockRowIndex, (m, n)) =>
//      sumRows += m
//      if (n != numCol) {
//        throw new SparkException(s"The MatrixBlock at ($blockRowIndex) has " +
//          dimensionMsg)
//      }
//    }
//    if (sumRows != numRows())
//      throw new SparkException(s"Wrong sum of number of rows!"+ dimensionMsg)
//  }
//
//  /** Caches the underlying RDD. */
//  def cache(): this.type = {
//    blocks.cache()
//    this
//  }
//
//  /** Persists the underlying RDD with the specified storage level. */
//  def persist(storageLevel: StorageLevel): this.type = {
//    blocks.persist(storageLevel)
//    this
//  }
//
////  /**
////   * Transpose this `BlockGraphMatrix`. Returns a new `BlockGraphMatrix` instance sharing the
////   * same underlying data. Is a lazy operation.
////   */
////  def transpose: BlockGraphMatrix = {
////    val transposedBlocks: RDD[((Int, Int), Matrix)] =
////      blocks.map { case ((blockRowIndex, blockColIndex), mat) =>
////        ((blockColIndex, blockRowIndex), mat.transpose)
////      }
////    new BlockGraphMatrix(transposedBlocks, colsPerBlock, rowsPerBlock, nCols, nRows)
////  }
//
//  /** Collects data and assembles a local dense breeze matrix (for test only). */
//  def toBreeze(): BDM[Double] = {
//    val localMat: Matrix = toLocalMatrix()
//    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
//  }
//
//  /**
//   * Collect the distributed matrix on the driver as a `DenseMatrix`.
//   */
//  def toLocalMatrix(): Matrix = {
//    require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
//      s"Int.MaxValue. Currently numRows: ${numRows()}")
//    require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
//      s"Int.MaxValue. Currently numCols: ${numCols()}")
//    require(numRows() * numCols() < Int.MaxValue, "The length of the values array must be " +
//      s"less than Int.MaxValue. Currently numRows * numCols: ${numRows() * numCols()}")
//    val m: Int = numRows().toInt
//    val n: Int = numCols().toInt
//
//    val localBlocks: Array[(Int, Matrix)] = blocks.collect()
//    val values = new Array[Double](m * n)
//    var offset = 0
//
//    localBlocks.foreach { case (blockRowIndex, submat) =>
//      submat.foreachActive { (i, j, v) =>
//        val indexOffset: Int = (i + offset) * m + j
//        values(indexOffset) = v
//      }
//      offset += submat.numRows
//    }
//    new DenseMatrix(m, n, values)
//  }
//
//  /**
//   * Adds the given block matrix `other` to `this` block matrix: `this + other`.
//   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
//   * values. If one of the blocks that are being added are instances of `SparseMatrix`,
//   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being added
//   * to a `DenseMatrix`. If two dense matrices are added, the output will also be a
//   * `DenseMatrix`.
//   */
//  def add(other: BlockGraphMatrix): BlockGraphMatrix =
//    blockMap(other, (x: BM[Double], y: BM[Double]) => x + y)
//
//  /**
//   * Subtracts the given block matrix `other` from `this` block matrix: `this - other`.
//   * The matrices must have the same size and matching `rowsPerBlock` and `colsPerBlock`
//   * values. If one of the blocks that are being subtracted are instances of `SparseMatrix`,
//   * the resulting sub matrix will also be a `SparseMatrix`, even if it is being subtracted
//   * from a `DenseMatrix`. If two dense matrices are subtracted, the output will also be a
//   * `DenseMatrix`.
//   */
//  def subtract(other: BlockGraphMatrix): BlockGraphMatrix =
//    blockMap(other, (x: BM[Double], y: BM[Double]) => x - y)
//
//  private def check(other: BlockGraphMatrix): Boolean = blockInfo sameElements other.blockInfo
//
//  /**
//   * For given matrices `this` and `other` of compatible dimensions and compatible block dimensions,
//   * it applies a binary function on their corresponding blocks.
//   *
//   * @param other  The second BlockGraphMatrix argument for the operator specified by `binMap`
//   * @param binMap A function taking two breeze matrices and returning a breeze matrix
//   * @return A [[BlockGraphMatrix]] whose blocks are the results of a specified binary map on blocks
//   *         of `this` and `other`.
//   *         Note: `blockMap` ONLY works for `add` and `subtract` methods and it does not support
//   *         operators such as (a, b) => -a + b
//   *         TODO: Make the use of zero matrices more storage efficient.
//   */
//  def blockMap(
//                other: BlockGraphMatrix,
//                binMap: (BM[Double], BM[Double]) => BM[Double]): BlockGraphMatrix = {
//    if (check(other)) {
//      val newBlocks: RDD[(Int, Matrix)] = blocks.cogroup(other.blocks, createPartitioner())
//        .map { case (blockRowIndex, (a, b)) =>
//          if (a.size > 1 || b.size > 1) {
//            throw new SparkException("There are multiple MatrixBlocks with indices: " +
//              s"($blockRowIndex). Please remove them.")
//          }
//          if (a.isEmpty) {
//            val zeroBlock: BM[Double] = BM.zeros[Double](b.head.numRows, b.head.numCols)
//            val result: BM[Double] = binMap(zeroBlock, b.head.asBreeze)
//            new MatrixBlock(blockRowIndex, Matrices.fromBreeze(result))
//          } else if (b.isEmpty) {
//            new MatrixBlock(blockRowIndex, a.head)
//          } else {
//            val result: BM[Double] = binMap(a.head.asBreeze, b.head.asBreeze)
//            new MatrixBlock(blockRowIndex, Matrices.fromBreeze(result))
//          }
//        }
//      new BlockGraphMatrix(newBlocks, numRows(), numCols())
//    } else {
//      throw new SparkException("Cannot perform on matrices with different block dimensions")
//    }
//  }
//
//  override def numRows(): Long = {
//    if (nRows <= 0L) estimateDim()
//    nRows
//  }
//
//  override def numCols(): Long = {
//    if (nCols <= 0L) estimateDim()
//    nCols
//  }
//
//  /** Estimates the dimensions of the matrix. */
//  private def estimateDim(): Unit = {
//    val (rows, cols) = blockInfo.map(_._2).reduce((x0, x1) => {
//      (x0._1 + x1._1, math.max(x0._2, x1._2))
//    })
//    if (nRows <= 0L) nRows = rows
//    assert(rows <= nRows, s"The number of rows $rows is more than claimed $nRows.")
//    if (nCols <= 0L) nCols = cols
//    assert(cols <= nCols, s"The number of columns $cols is more than claimed $nCols.")
//  }
//
//  def createPartitioner(): GridPartitioner =
//    GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.partitions.length)
//
//  /**
//   * Left multiplies this [[BlockGraphMatrix]] to `other`, another [[BlockGraphMatrix]]. The `colsPerBlock`
//   * of this matrix must equal the `rowsPerBlock` of `other`. If `other` contains
//   * `SparseMatrix`, they will have to be converted to a `DenseMatrix`. The output
//   * [[BlockGraphMatrix]] will only consist of blocks of `DenseMatrix`. This may cause
//   * some performance issues until support for multiplying two sparse matrices is added.
//   *
//   * @note The behavior of multiply has changed in 1.6.0. `multiply` used to throw an error when
//   *       there were blocks with duplicate indices. Now, the blocks with duplicate indices will be added
//   *       with each other.
//   */
//  def multiply(other: BlockGraphMatrix): BlockGraphMatrix = {
//    multiply(other, 1)
//  }
//
//  /**
//   * Left multiplies this [[BlockGraphMatrix]] to `other`, another [[BlockGraphMatrix]]. The `colsPerBlock`
//   * of this matrix must equal the `rowsPerBlock` of `other`. If `other` contains
//   * `SparseMatrix`, they will have to be converted to a `DenseMatrix`. The output
//   * [[BlockGraphMatrix]] will only consist of blocks of `DenseMatrix`. This may cause
//   * some performance issues until support for multiplying two sparse matrices is added.
//   * Blocks with duplicate indices will be added with each other.
//   *
//   * @param other           Matrix `B` in `A * B = C`
//   * @param numMidDimSplits Number of splits to cut on the middle dimension when doing
//   *                        multiplication. For example, when multiplying a Matrix `A` of
//   *                        size `m x n` with Matrix `B` of size `n x k`, this parameter
//   *                        configures the parallelism to use when grouping the matrices. The
//   *                        parallelism will increase from `m x k` to `m x k x numMidDimSplits`,
//   *                        which in some cases also reduces total shuffled data.
//   */
//  def multiply(
//                other: BlockGraphMatrix,
//                numMidDimSplits: Int): BlockGraphMatrix = {
//    require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
//      s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
//      "think they should be equal, try setting the dimensions of A and B explicitly while " +
//      "initializing them.")
//    require(numMidDimSplits > 0, "numMidDimSplits should be a positive integer.")
//    if (colsPerBlock == other.rowsPerBlock) {
//      val resultPartitioner: GridPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
//        math.max(blocks.partitions.length, other.blocks.partitions.length))
//      val (leftDestinations, rightDestinations) = simulateMultiply(other, resultPartitioner, numMidDimSplits)
//      // Each block of A must be multiplied with the corresponding blocks in the columns of B.
//      val flatA: RDD[(Int, (Int, Int, Matrix))] = blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
//        val destinations: Set[Int] = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
//        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
//      }
//      // Each block of B must be multiplied with the corresponding blocks in each row of A.
//      val flatB: RDD[(Int, (Int, Int, Matrix))] = other.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
//        val destinations: Set[Int] = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
//        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
//      }
//      val intermediatePartitioner: Partitioner = new Partitioner {
//        override def numPartitions: Int = resultPartitioner.numPartitions * numMidDimSplits
//
//        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
//      }
//      val newBlocks: RDD[((Int, Int), Matrix)] = flatA.cogroup(flatB, intermediatePartitioner).flatMap { case (_, (a, b)) =>
//        a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
//          b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
//            val C: DenseMatrix = rightBlock match {
//              case dense: DenseMatrix => leftBlock.multiply(dense)
//              case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
//              case _ =>
//                throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
//            }
//            ((leftRowIndex, rightColIndex), C.asBreeze)
//          }
//        }
//      }.reduceByKey(resultPartitioner, (a, b) => a + b).mapValues(Matrices.fromBreeze)
//      // TODO: Try to use aggregateByKey instead of reduceByKey to get rid of intermediate matrices
//      new BlockGraphMatrix(newBlocks, rowsPerBlock, other.colsPerBlock, numRows(), other.numCols())
//    } else {
//      throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
//        s"A.colsPerBlock: $colsPerBlock, B.rowsPerBlock: ${other.rowsPerBlock}")
//    }
//  }
//
////  def
//
//  /**
//   * Simulate the multiplication with just block indices in order to cut costs on communication,
//   * when we are actually shuffling the matrices.
//   * The `colsPerBlock` of this matrix must equal the `rowsPerBlock` of `other`.
//   * Exposed for tests.
//   *
//   * @param other       The BlockGraphMatrix to multiply
//   * @param partitioner The partitioner that will be used for the resulting matrix `C = A * B`
//   * @return A tuple of [[BlockDestinations]]. The first element is the Map of the set of partitions
//   *         that we need to shuffle each blocks of `this`, and the second element is the Map for
//   *         `other`.
//   */
//  def simulateMultiply(
//                        other: BlockGraphMatrix,
//                        partitioner: GridPartitioner,
//                        midDimSplitNum: Int): (BlockDestinations, BlockDestinations) = {
//    val leftMatrix = blockInfo.keys.collect()
//    val rightMatrix = other.blockInfo.keys.collect()
//
//    val rightCounterpartsHelper = rightMatrix.groupBy(_._1).mapValues(_.map(_._2))
//    val leftDestinations = leftMatrix.map { case (rowIndex, colIndex) =>
//      val rightCounterparts = rightCounterpartsHelper.getOrElse(colIndex, Array.empty[Int])
//      val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b)))
//      val midDimSplitIndex = colIndex % midDimSplitNum
//      ((rowIndex, colIndex),
//        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
//    }.toMap
//
//    val leftCounterpartsHelper = leftMatrix.groupBy(_._2).mapValues(_.map(_._1))
//    val rightDestinations = rightMatrix.map { case (rowIndex, colIndex) =>
//      val leftCounterparts = leftCounterpartsHelper.getOrElse(rowIndex, Array.empty[Int])
//      val partitions = leftCounterparts.map(b => partitioner.getPartition((b, colIndex)))
//      val midDimSplitIndex = rowIndex % midDimSplitNum
//      ((rowIndex, colIndex),
//        partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
//    }.toMap
//
//    (leftDestinations, rightDestinations)
//  }
//}
