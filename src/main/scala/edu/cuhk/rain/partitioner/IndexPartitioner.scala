package edu.cuhk.rain.partitioner

import org.apache.spark.Partitioner
import edu.cuhk.rain.util.lowerBound

class IndexPartitioner(val thresholds: Array[Int]) extends Partitioner{
  val partitions: Int = thresholds.length
  val ind2part: Array[Int] = new Array[Int](partitions)

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    require(key.isInstanceOf[Int])
    lowerBound(thresholds, key.asInstanceOf[Int])
  }
}
