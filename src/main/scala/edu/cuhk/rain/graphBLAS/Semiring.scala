package edu.cuhk.rain.graphBLAS

class Semiring[OUT, IN](
                         val add_op: Monoid[OUT],
                         val mul_Op: BinaryOp[OUT, IN]
                       ) extends Serializable {

}

object Semiring {
  lazy val semiringPlusMul = new Semiring[Double, Double](Monoid.monoidPlus, BinaryOp.binaryMul)

  lazy val semiringPlusMin = new Semiring[Double, Double](Monoid.monoidPlus, BinaryOp.binaryMin)

  lazy val semiringMinMul = new Semiring[Double, Double](Monoid.monoidMin, BinaryOp.binaryMul)

  lazy val semiringMaxfracPlus = new Semiring[Double, Double](Monoid.monoidMaxfrac, BinaryOp.binaryPlus)
}