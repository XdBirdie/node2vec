package edu.cuhk.rain.graphBLAS

class Monoid[T](
                 val binaryOp: BinaryOp[T, T],
                 val identity: T
               ) extends Serializable {
  def apply(x0: T, x1: T): T = {
    binaryOp(x0, x1)
  }

  def apply(x0: T): T = {
    binaryOp(x0, identity)
  }
}

object Monoid {
  lazy val monoidPlus = new Monoid[Double](BinaryOp.binaryPlus, 0)
  //  lazy val monoidMinus = new Monoid[Double](BinaryOp.binaryMinus, )
  lazy val monoidMul = new Monoid[Double](BinaryOp.binaryMul, 1)

  lazy val monoidMin = new Monoid[Double](BinaryOp.binaryMin, Double.MaxValue)
  lazy val monoidMax = new Monoid[Double](BinaryOp.binaryMax, Double.MinValue)

  lazy val monoidMaxfrac = new Monoid[Double](BinaryOp.binaryMaxfrac, 0)
}
