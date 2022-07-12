package betweeness

import com.jni.BLAS
import org.apache.spark.ml.linalg.SparseMatrix
object blas {
  def vxmMinPlus(sparseMatrix: SparseMatrix,vec:Array[(Int,Double)]): Array[Double]={
    val b = new BLAS();
    var id:Int = 0
    val dst=Array[Int](sparseMatrix.rowIndices.length)
    for(i <- sparseMatrix.rowIndices.indices){
      while(i==sparseMatrix.colPtrs(id+1))
        id+=1
      dst(i)=id
    }
    b.vxm_minPlus(sparseMatrix.rowIndices,dst,sparseMatrix.values,vec.map(f=>f._1),vec.map(f=>f._2),sparseMatrix.numRows,sparseMatrix.numCols)
  }
  def vxmPlusTimes(sparseMatrix: SparseMatrix,vec:Array[(Int,Double)]): Array[Double]={
    val b = new BLAS();
    var id:Int = 0
    val dst=Array[Int](sparseMatrix.rowIndices.length)
    for(i <- sparseMatrix.rowIndices.indices){
      while(i==sparseMatrix.colPtrs(id+1))
        id+=1
      dst(i)=id
    }
    b.vxm_plusTimes(sparseMatrix.rowIndices,sparseMatrix.colPtrs,sparseMatrix.values,vec.map(f=>f._1),vec.map(f=>f._2),sparseMatrix.numRows,sparseMatrix.numCols)
  }
}
