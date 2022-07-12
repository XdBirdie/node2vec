package betweeness

import org.apache.spark.ml.linalg.SparseMatrix

object Main extends App {
  val tmp :Array[(Int,Double)]=Array((0,0.0))
  val m:SparseMatrix=new SparseMatrix(2,2,Array(0,0,1),Array(0),Array(1.0))
  print(blas.vxmMinPlus(m, tmp).mkString("Array(", ", ", ")"))
}
