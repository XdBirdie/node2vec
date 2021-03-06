package edu.cuhk.rain.partitioner

import edu.cuhk.rain.util.lowerBound
import org.apache.spark.Partitioner

class IndexPartitioner(val thresholds: Array[Int]) extends Partitioner {
  val partitions: Int = thresholds.length
  val ind2part: Array[Int] = new Array[Int](partitions)

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    require(key.isInstanceOf[Int])
    lowerBound(thresholds, key.asInstanceOf[Int])
  }

  override def toString: String =
    s"IndexPartitioner: [${thresholds.mkString(", ")}]"
}
