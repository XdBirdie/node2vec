def lowerBound(a: Array[Int], x: Int): Int = {
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

var a = Array(0, 2, 4, 6, 8, 10, 12, 14)
for (i <- -1 to a.max + 2) {
  println(lowerBound(a, i))
}