package edu.cuhk.rain.test

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object SparseVector {
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): SparseVector =
    new SparseVector(size, indices, values)

  def sparse(size: Int, elements: Seq[(Int, Double)]): SparseVector = {
    require(size > 0, "The size of the requested sparse vector must be greater than 0.")

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev: Int = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size, s"You may not write an element to index $prev because the declared " +
      s"size of your vector is $size")

    new SparseVector(size, indices.toArray, values.toArray)
  }

  val MAX_HASH_NNZ = 128
}


class SparseVector (val size: Int,
                    val indices: Array[Int],
                    val values: Array[Double]) extends Serializable {
  def toArray: Array[Double] = {
    val data = new Array[Double](size)
    var i = 0
    val nnz: Int = indices.length
    while (i < nnz) {
      data(indices(i)) = values(i)
      i += 1
    }
    data
  }

  def apply(i: Int): Double = {
    require(i >= 0 && i <= size)
    var l = 0
    var r: Int = indices.length
    while (l < r) {
      val m: Int = (r - l) / 2 + l
      if (indices(m) == i) return values(m)
      else if (indices(m) < i) l = m + 1
      else r = m - 1
    }
    0
  }

  def foreachActive(f: (Int, Double) => Unit): Unit = {
    var i = 0
    val localValuesSize: Int = values.length
    val localIndices: Array[Int] = indices
    val localValues: Array[Double] = values

    while (i < localValuesSize) {
      f(localIndices(i), localValues(i))
      i += 1
    }
  }

  def mapActive[T: ClassTag](f: (Int, Double) => T): Array[T] = {
    indices.zip(values).map{case (ind, value) => f(ind, value)}
  }

  def multiply(s: Double): SparseVector = {
    new SparseVector(size, indices.clone(), indices.map(_*s))
  }

  def add(other: SparseVector): SparseVector = {
    if (indices sameElements other.indices) {
      val res: Array[Double] = values.clone()
      var i = 0
      val len: Int = res.length
      while (i < len) {
        res(i) += other.values(i)
        i += 1
      }
      return new SparseVector(size, indices.clone(), res)
    }
    var i = 0
    var j = 0
    val l1: Int = indices.length
    val l2: Int = other.indices.length

    val vbuf = new ArrayBuffer[Double]()
    val ibuf = new ArrayBuffer[Int]()

    while (i < l1 && j < l2) {
      val indi: Int = indices(i)
      val indj: Int = other.indices(j)

      if (indi == indj) {
        vbuf.append(values(i) + other.values(j))
        ibuf.append(indi)
        i += 1
        j += 1
      } else if (indi < indj) {
        vbuf.append(values(i))
        ibuf.append(indi)
        i += 1
      } else {
        vbuf.append(other.values(j))
        ibuf.append(indj)
        j += 1
      }
    }

    while (i < l1) {
      ibuf.append(indices(i))
      vbuf.append(values(i))
      i += 1
    }

    while (j < l2) {
      ibuf.append(other.indices(j))
      vbuf.append(other.values(j))
      j += 1
    }

    new SparseVector(ibuf.size, ibuf.toArray, vbuf.toArray)
  }
}
