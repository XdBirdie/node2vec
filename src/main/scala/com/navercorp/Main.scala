package com.navercorp

import com.navercorp.N2V.{N2VBaseline, N2VBroadcast, N2VJoin2, N2VOne, N2VPartition, Node2Vec}

import java.io.Serializable
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import com.navercorp.lib.AbstractParams
import com.navercorp.util.TimeRecorder
import org.apache.spark.rdd.RDD

object Main {
  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding = Value
  }
  object Version extends Enumeration {
    type Version = Value
    val baseline, partition, broadcast, join2, one = Value
  }
  import Command._
  import Version._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartitions: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
                    minCount: Int = 0,
                    walkLength: Int = 80,
                    numWalks: Int = 10,
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false,
                    degree: Int = 30,
                    indexed: Boolean = true,
                    nodePath: String = null,
                    input: String = null,
                    output: String = null,
                    logPath: String = "./time-record.log",
                    cmd: Command = Command.node2vec,
                    version: Version = Version.partition) extends AbstractParams[Params] with Serializable
  val defaultParams: Params = Params()

  val parser: OptionParser[Params] = new OptionParser[Params]("Node2Vec_Spark") {
    head("Node2vec Spark")
    opt[Int]("iter")
      .text(s"iter: ${defaultParams.iter}")
      .action((x: Int, c: Params) => c.copy(iter = x))
    opt[Int]("walkLength")
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x: Int, c: Params) => c.copy(walkLength = x))
    opt[Int]("numWalks")
      .text(s"numWalks: ${defaultParams.numWalks}")
      .action((x: Int, c: Params) => c.copy(numWalks = x))
    opt[Double]("p")
      .text(s"return parameter p: ${defaultParams.p}")
      .action((x: Double, c: Params) => c.copy(p = x))
    opt[Double]("q")
      .text(s"in-out parameter q: ${defaultParams.q}")
      .action((x: Double, c: Params) => c.copy(q = x))
    opt[Int]("minCount")
      .text(s"minCount: ${defaultParams.minCount}")
      .action((x: Int, c: Params) => c.copy(minCount = x))
    opt[Boolean]("weighted")
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x: Boolean, c: Params) => c.copy(weighted = x))
    opt[Boolean]("directed")
      .text(s"directed: ${defaultParams.directed}")
      .action((x: Boolean, c: Params) => c.copy(directed = x))
    opt[Int]("degree")
      .text(s"degree: ${defaultParams.degree}")
      .action((x: Int, c: Params) => c.copy(degree = x))
    opt[Boolean]("indexed")
      .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
      .action((x: Boolean, c: Params) => c.copy(indexed = x))
    opt[String]("nodePath")
      .text("Input node2index file path: empty")
      .action((x: String, c: Params) => c.copy(nodePath = x))
    opt[String]("logPath")
      .text(s"Log path: ${defaultParams.logPath}")
      .action((x: String, c: Params) => c.copy(logPath = x))
    opt[String]("input")
      .required()
      .text("Input edge file path: empty")
      .action((x: String, c: Params) => c.copy(input = x))
    opt[String]("output")
      .required()
      .text("Output path: empty")
      .action((x: String, c: Params) => c.copy(output = x))
    opt[String]("cmd")
      .required()
      .text(s"command: ${defaultParams.cmd.toString}")
      .action((x: String, c: Params) => c.copy(cmd = Command.withName(x)))
    opt[String]("version")
      .text(s"version: ${defaultParams.version.toString}")
      .action((x: String, c: Params) => c.copy(version = Version.withName(x)))
    opt[Int]("partitions")
      .text(s"partitions: ${defaultParams.numPartitions}")
      .action((x: Int, c: Params) => c.copy(numPartitions=x))

    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class com.nhn.sunny.vegapunk.ml.model.Node2vec \
      """.stripMargin +
              s"|   --lr ${defaultParams.lr}" +
              s"|   --iter ${defaultParams.iter}" +
              s"|   --numPartition ${defaultParams.numPartitions}" +
              s"|   --dim ${defaultParams.dim}" +
              s"|   --window ${defaultParams.window}" +
              s"|   --input <path>" +
              s"|   --node <nodeFilePath>" +
              s"|   --output <path>"
    )
  }
  
  def main(args: Array[String]):Unit = {
    TimeRecorder.setup()

    val option: Option[Params] = parser.parse(args, defaultParams)
    val param: Params = option.get

    val conf: SparkConf = new SparkConf().setAppName("com.navercorp.N2V.Node2Vec")
    val context: SparkContext = new SparkContext(conf)
    context.setLogLevel("WARN")

    val N2V: Node2Vec = param.version match {
      case Version.baseline => N2VBaseline
      case Version.partition => N2VPartition
      case Version.broadcast => N2VBroadcast
      case Version.join2 => N2VJoin2
      case Version.one => N2VOne
    }

    N2V.setup(context, param)
    println(param)

    param.cmd match {
      case Command.node2vec =>
        N2V.load().initTransitionProb().randomWalk().embedding().save()
      case Command.randomwalk =>
        N2V.load().initTransitionProb().randomWalk().saveRandomPath()
      case Command.embedding =>
        val randomPaths: RDD[Iterable[String]] = word2vec.setup(context, param).read(param.input)
        word2vec.fit(randomPaths).save(param.output)
        N2V.loadNode2Id(param.nodePath).saveVectors()
    }

    TimeRecorder.show()
    TimeRecorder.save(param.logPath)
  }
}
