package edu.cuhk.rain.jni

object Operation {
  @native def vxm(
                 u: Array[Int],
                 v: Array[Int],
                 w: Array[Double]

                 ): Unit
}
