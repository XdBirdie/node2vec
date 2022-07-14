package edu.cuhk.rain

import edu.cuhk.rain.distributed.Distributed
import edu.cuhk.rain.partitioner.Partitioner
import edu.cuhk.rain.util.ParamsPaser._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.sys.exit

object APP {
  def start(param: Params):Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("edu.cuhk.rain.node2vec")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("hdfs://~/checkpoint/")

    param.cmd match {
      case Command.node2vec =>
      case Command.randomwalk =>
      case Command.embedding =>
      case Command.partition =>
    }

  }
  
  def main(args: Array[String]):Unit = {
    val conf: SparkConf = new SparkConf().setAppName("edu.cuhk.rain.node2vec").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    val params: Params = Params(
      partitions = 4,
      weighted = false,
      directed = false,
      input = "./data/karate.edgelist",
      cmd = Command.partition
    )

    Partitioner.setup(context, params).partition()
//    Distributed.setup(context, params).start()

//    val option: Option[Params] = parse(args)
//    option match {
//      case Some(param) => start(param)
//      case _ => exit(1)
//    }
  }
}
