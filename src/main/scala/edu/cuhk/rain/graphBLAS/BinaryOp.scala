package edu.cuhk.rain.graphBLAS

class BinaryOp[OUT, IN](
                         val op: (IN, IN) => OUT
                       ) extends Serializable {
  def apply(in1: IN, in2: IN): OUT = op(in1, in2)
}

case object BinaryOp {
  lazy val binaryPlus = new BinaryOp[Double, Double](_ + _)
  lazy val binaryMinus = new BinaryOp[Double, Double](_ - _)
  lazy val binaryMul = new BinaryOp[Double, Double](_ * _)
  lazy val binaryDiv = new BinaryOp[Double, Double](_ / _)

  lazy val binaryMin = new BinaryOp[Double, Double](math.min)
  lazy val binaryMax = new BinaryOp[Double, Double](math.max)

  lazy val binaryMaxfrac = new BinaryOp[Double, Double]((x, y) => {
    val px: Double = x - x.floor
    val py: Double = y - y.floor
    if (px >= py) x else y
  })

  lazy val binaryFirst = new BinaryOp[Double, Double]((u, _) => u)
  lazy val binarySecond = new BinaryOp[Double, Double]((_, v) => v)
}
