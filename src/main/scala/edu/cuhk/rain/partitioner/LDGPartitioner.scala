package edu.cuhk.rain.partitioner

import org.apache.spark.Partitioner

class LDGPartitioner (val numPartition: Int, val numVertex: Int){
  val partitions: Array[Partition] = new Array[Partition](numPartition)
  val capacity: Int = (numVertex.toDouble / numPartition).ceil.toInt
  for (i <- 0 until numPartition) {
    partitions(i) = new Partition(i, capacity)
  }

  def addNode(u: Long, vs: Array[Long]): Unit = {
    var maxw: Double = Double.MinValue
    var ind = 0
    for (i <- 0 until numPartition) {
      val w: Double = partitions(i).calc(vs)
      if (w > maxw) {
        maxw = w
        ind = i
      }
    }
    partitions(ind).addNode(u)
  }

  def node2partition: Map[Long, Int] = {
    partitions.foreach(x => println(x.size))
    partitions.zipWithIndex.flatMap{case (partition, id) =>
      partition.nodes.map((_, id))
    }.toMap
  }

  def node2id: Array[(Long, Long)] = {
    var id = 0
    partitions.flatMap(_.nodes.map{node =>
        val res: (Long, Long) = (node, id)
        id += 1
        res
      }
    )
  }

  def partitioner: Partitioner = {
    val a = new Array[Int](numPartition)
    var cnt = 0
    for (i <- 0 until numPartition) {
      a(i) = cnt
      cnt += partitions(i).size
    }
    new IndexPartitioner(a)
  }

}
