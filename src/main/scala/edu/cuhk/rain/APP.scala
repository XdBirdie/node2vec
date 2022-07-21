package edu.cuhk.rain

import edu.cuhk.rain.distributed.Distributed
import edu.cuhk.rain.partitioner.PartitionerTester
import edu.cuhk.rain.randomwalk.RandomWalk
import edu.cuhk.rain.util.ParamsPaser
import edu.cuhk.rain.util.ParamsPaser._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object APP extends Logging{
  def start(param: Params): Unit = {
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

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("edu.cuhk.rain.node2vec").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

//    val params: Params = Params(
//      partitions = 4,
//      weighted = false,
//      directed = false,
////      input = "./data/karate.edgelist",
//      input = "./data/BlogCatalog",
//      walkLength = 20,
//      numWalks = 5,
//      cmd = Command.partition
//    )
//
//    RandomWalk.setup(context, params).start().debug()

    ParamsPaser.parse(args) match {
      case Some(params) => RandomWalk.setup(context, params).start() //.debug()
      case _ => logError("error params!")
    }
//    Partitioner.setup(context, params).partition()
//        Distributed.setup(context, params).start()

    //    val option: Option[Params] = parse(args)
    //    option match {
    //      case Some(param) => start(param)
    //      case _ => exit(1)
    //    }
  }
}
