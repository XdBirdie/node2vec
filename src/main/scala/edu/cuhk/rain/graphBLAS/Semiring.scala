package edu.cuhk.rain.graphBLAS

class Semiring[OUT, IN] (
               val add_op: Monoid[OUT],
               val mul_Op: BinaryOp[OUT, IN]
               ) extends Serializable {

}

object Semiring {
  lazy val semiringPlusAdd = new Semiring[Double, Double](Monoid.monoidPlus, BinaryOp.binaryMul)
}