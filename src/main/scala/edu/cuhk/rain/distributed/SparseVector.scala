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

sealed class SparseVector(
                           val size: Int,
                           _values: Array[(Int, Double)]
                         ) extends Serializable {
  val values: Array[(Int, Double)] = _values.sorted

  def this(size: Int, indices: Array[Int], values: Array[Double]) {
    this(size, indices zip values)
  }

  def isEmpty: Boolean = {
    if (values == null || values.isEmpty) true
    else {
      for ((_, v) <- values)
        if (v != 0.0) return false
      true
    }
  }

  def toArray: Array[Double] = {
    val data = new Array[Double](size)
    values.foreach { case (i, v) => data(i) = v }
    data
  }

  def apply(i: Int): Double = {
    require(i >= 0 && i < size)
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
    coOperate(other, Monoid.monoidPlus)
  }

  def multiply(other: SparseVector): SparseVector = {
    coOperate(other, Monoid.monoidMul)
  }

  def div(other: SparseVector): SparseVector = {
    coOperate(other, new Monoid[Double](
      new BinaryOp[Double, Double](
        (x: Double, y: Double) =>
          if (x == 0.0) 0.0
          else if (y == 0.0) 0.0
          else x / y
        ),
      0.0))
  }

  def coOperate(other: SparseVector, monoid: Monoid[Double]): SparseVector = {
    require(size == other.size)

    if (sameIndices(other.values)) {
      var i = 0
      val sum: Array[(Int, Double)] = values.map { case (ind, v) =>
        val res: (Int, Double) = (ind, monoid(v, other.values(i)._2))
        i += 1
        res
      }.filter(_._2 != 0)
      return new SparseVector(size, sum)
    }

    var (i, j) = (0, 0)
    val (l1, l2) = (values.length, other.values.length)

    val res = new ArrayBuffer[(Int, Double)]()
    while (i < l1 && j < l2) {
      val indi: Int = values(i)._1
      val indj: Int = other.values(j)._1

      if (indi == indj) {
        val t: Double = monoid(values(i)._2, other.values(j)._2)
        if (t != 0.0) res.append((indi, t))
        i += 1; j += 1
      } else if (indi < indj) {
        val t: Double = monoid(values(i)._2)
        if (t != 0.0) res.append((indi, t))
        i += 1
      } else {
        val t: Double = monoid(other.values(j)._2)
        if (t != 0.0) res.append((indi, t))
        res.append((indj, t))
        j += 1
      }
    }
    while (i < l1) {
      val t: Double = monoid(values(i)._2)
      if (t != 0.0) res.append((values(i)._1, t))
      i += 1
    }
    while (j < l2) {
      val t: Double = monoid(other.values(j)._2)
      if (t != 0.0) res.append((other.values(j)._1, t))
      j += 1
    }

    new SparseVector(size, res.toArray)
  }

  def maskBy(mask: Array[Int], positive: Boolean): SparseVector = {
    val m: Array[Int] = mask.sorted
    var (i, j) = (0, 0)
    val buf = new ArrayBuffer[(Int, Double)]()

    while (i < values.length && j < m.length) {
      val ind: Int = values(i)._1
      while (j < m.length && m(j) < ind) j += 1
      if (j < m.length) {
        if (m(j) == ind) {
          if (positive) buf.append(values(i))
        } else if (!positive) buf.append(values(i))
      }
      i += 1
    }

    new SparseVector(size, buf.toArray)
  }

  private def sameIndices(other: Array[(Int, Double)]): Boolean = {
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

class ZeroSparseVector(size: Int) extends SparseVector(size, Array.empty)