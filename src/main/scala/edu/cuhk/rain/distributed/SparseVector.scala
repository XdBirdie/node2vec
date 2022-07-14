package edu.cuhk.rain.distributed

import edu.cuhk.rain.graphBLAS.{BinaryOp, Monoid}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object SparseVector {
  val MAX_HASH_NNZ = 128

  def ones(size: Int, indices: Array[Int]): SparseVector =
    new SparseVector(size, indices zip Array.fill(indices.length)(1.0))

  def ids(size: Int, indices: Array[Int]): SparseVector =
    new SparseVector(size, indices.map(ind => (ind, ind.toDouble)))

  def random(size: Int, indices: Array[Int]): SparseVector =
    new SparseVector(size, indices zip Array.fill(indices.length)(math.random))

  def random(size: Int): SparseVector =
    random(size, Array.range(0, size))

  def apply(size: Int, indices: Array[Int], values: Array[Double]): SparseVector =
    new SparseVector(size, indices.zip(values))

  def apply(size: Int, elements: Seq[(Int, Double)]): SparseVector = {
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
}


//class SparseVector (val size: Int,
//                    val indices: Array[Int],
//                    val values: Array[Double]) extends Serializable {
//
//  def toArray: Array[Double] = {
//    val data = new Array[Double](size)
//    var i = 0
//    val nnz: Int = indices.length
//    while (i < nnz) {
//      data(indices(i)) = values(i)
//      i += 1
//    }
//    data
//  }
//
//  def apply(i: Int): Double = {
//    require(i >= 0 && i <= size)
//    var l = 0
//    var r: Int = indices.length
//    while (l < r) {
//      val m: Int = (r - l) / 2 + l
//      if (indices(m) == i) return values(m)
//      else if (indices(m) < i) l = m + 1
//      else r = m - 1
//    }
//    0
//  }
//
//  def foreachActive(f: (Int, Double) => Unit): Unit = {
//    var i = 0
//    val localValuesSize: Int = values.length
//    val localIndices: Array[Int] = indices
//    val localValues: Array[Double] = values
//
//    while (i < localValuesSize) {
//      f(localIndices(i), localValues(i))
//      i += 1
//    }
//  }
//
//  def mapActive[T: ClassTag](f: (Int, Double) => T): Array[T] = {
//    indices.zip(values).map{case (ind, value) => f(ind, value)}
//  }
//
//  def multiply(s: Double): SparseVector = {
//    new SparseVector(size, indices.clone(), values.map(_*s))
//  }
//
//  def multiply(s: Double, mul: BinaryOp[Double, Double]): SparseVector = {
//    new SparseVector(size, indices.clone(), values.map(x => mul(s, x)))
//  }
//
//  def add(other: SparseVector, add: Monoid[Double]): SparseVector = {
//    require(size == other.size)
//
//    if (indices sameElements other.indices) {
//      val res: Array[Double] = Array.ofDim(values.length)
//      var i = 0
//      val len: Int = res.length
//      while (i < len) {
//        res(i) = add(values(i), other.values(i))
//        i += 1
//      }
//      return new SparseVector(size, indices.clone(), res)
//    }
//
//    var (i, j) = (0, 0)
//    val (l1, l2) = (indices.length, other.indices.length)
//    val vbuf = new ArrayBuffer[Double]()
//    val ibuf = new ArrayBuffer[Int]()
//    while (i < l1 && j < l2) {
//      val indi: Int = indices(i)
//      val indj: Int = other.indices(j)
//
//      if (indi == indj) {
//        vbuf.append(add(values(i), other.values(j)))
//        ibuf.append(indi)
//        i += 1
//        j += 1
//      } else if (indi < indj) {
//        vbuf.append(add(values(i)))
//        ibuf.append(indi)
//        i += 1
//      } else {
//        vbuf.append(add(other.values(j)))
//        ibuf.append(indj)
//        j += 1
//      }
//    }
//
//    while (i < l1) {
//      ibuf.append(indices(i))
//      vbuf.append(add(values(i)))
//      i += 1
//    }
//
//    while (j < l2) {
//      ibuf.append(other.indices(j))
//      vbuf.append(add(other.values(j)))
//      j += 1
//    }
//
//    new SparseVector(size, ibuf.toArray, vbuf.toArray)
//  }
//
//  def add(other: SparseVector): SparseVector = {
//    add(other, Monoid.monoidPlus)
//  }
//
//  override def toString: String = {
//    s"size: ${size}, values: [${indices.zip(values).mkString(", ")}]"
//  }
//}

class SparseVector(val size: Int,
                   val values: Array[(Int, Double)]) extends Serializable {

  def this(size: Int, indices: Array[Int], values: Array[Double]) {
    this(size, indices zip values)
  }

  def toArray: Array[Double] = {
    val data = new Array[Double](size)
    values.foreach { case (i, v) => data(i) = v }
    data
  }

  def apply(i: Int): Double = {
    require(i >= 0 && i <= size)
    var l = 0
    var r: Int = values.length
    while (l < r) {
      val m: Int = (r - l) / 2 + l
      if (values(m)._1 == i) return values(m)._2
      else if (values(m)._1 < i) l = m + 1
      else r = m - 1
    }
    0
  }

  def foreachActive(f: (Int, Double) => Unit): Unit = {
    values.foreach { case (i, v) => f(i, v) }
  }

  def mapActive[T: ClassTag](f: (Int, Double) => T): Array[T] = {
    values.map { case (ind, value) => f(ind, value) }
  }

  def multiply(s: Double): SparseVector = {
    new SparseVector(size, values.map { case (i, v) => (i, s * v) })
  }

  def multiply(s: Double, mul: BinaryOp[Double, Double]): SparseVector = {
    new SparseVector(size, values.map { case (i, v) => (i, mul(s, v)) })
  }

  def add(other: SparseVector): SparseVector = {
    add(other, Monoid.monoidPlus)
  }

  def add(other: SparseVector, add: Monoid[Double]): SparseVector = {
    require(size == other.size)

    if (sameIndices(other.values)) {
      var i = 0
      val sum: Array[(Int, Double)] = values.map { case (ind, v) =>
        val res: (Int, Double) = (ind, add(v, other.values(i)._2))
        i += 1
        res
      }
      return new SparseVector(size, sum)
    }

    var (i, j) = (0, 0)
    val (l1, l2) = (values.length, other.values.length)

    val res = new ArrayBuffer[(Int, Double)]()
    while (i < l1 && j < l2) {
      val indi: Int = values(i)._1
      val indj: Int = other.values(j)._1

      if (indi == indj) {
        res.append((indi, add(values(i)._2, other.values(j)._2)))
        i += 1
        j += 1
      } else if (indi < indj) {
        res.append((indi, add(values(i)._2)))
        i += 1
      } else {
        res.append((indj, add(other.values(j)._2)))
        j += 1
      }
    }
    while (i < l1) {
      res.append((values(i)._1, add(values(i)._2)))
      i += 1
    }

    while (j < l2) {
      res.append((other.values(j)._1, add(other.values(j)._2)))
      j += 1
    }

    new SparseVector(size, res.toArray)
  }

  def sameIndices(other: Array[(Int, Double)]): Boolean = {
    if (values.length != other.length) return false
    val len: Int = values.length
    var i = 0
    while (i < len) {
      if (values(i)._1 != other(i)._1) return false
      i += 1
    }
    true
  }

  override def toString: String = {
    s"size: ${size}, values: [${values.mkString(", ")}]"
  }
}
