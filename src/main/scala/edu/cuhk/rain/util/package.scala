package edu.cuhk.rain

package object util {


  def lowerBound(a: Array[Int], x: Int): Int = {
    if (x < a(0)) return -1
    var l = 0
    var r: Int = a.length
    while (l < r - 1) {
      val m: Int = (l + r) / 2
      if (a(m) == x) return m
      else if (a(m) > x) r = m
      else l = m
    }
    l
  }
}
