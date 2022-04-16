package com.navercorp

import java.io.Serializable
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import com.navercorp.lib.AbstractParams
import com.navercorp.util.TimeRecorder

object Main {
  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding = Value
  }
  object Version extends Enumeration {
    type Version = Value
    val baseline, partition, broadcast, join2 = Value
  }
  import Command._
  import Version._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartition: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
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
                    cmd: Command = Command.node2vec,
                    version: Version = Version.partition) extends AbstractParams[Params] with Serializable
  val defaultParams: Params = Params()
  
  val parser: OptionParser[Params] = new OptionParser[Params]("Node2Vec_Spark") {
    head("Node2vec Spark")
    opt[Int]("walkLength")
            .text(s"walkLength: ${defaultParams.walkLength}")
            .action((x, c) => c.copy(walkLength = x))
    opt[Int]("numWalks")
            .text(s"numWalks: ${defaultParams.numWalks}")
            .action((x, c) => c.copy(numWalks = x))
    opt[Double]("p")
            .text(s"return parameter p: ${defaultParams.p}")
            .action((x, c) => c.copy(p = x))
    opt[Double]("q")
            .text(s"in-out parameter q: ${defaultParams.q}")
            .action((x, c) => c.copy(q = x))
    opt[Boolean]("weighted")
            .text(s"weighted: ${defaultParams.weighted}")
            .action((x, c) => c.copy(weighted = x))
    opt[Boolean]("directed")
            .text(s"directed: ${defaultParams.directed}")
            .action((x, c) => c.copy(directed = x))
    opt[Int]("degree")
            .text(s"degree: ${defaultParams.degree}")
            .action((x, c) => c.copy(degree = x))
    opt[Boolean]("indexed")
            .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
            .action((x, c) => c.copy(indexed = x))
    opt[String]("nodePath")
            .text("Input node2index file path: empty")
            .action((x, c) => c.copy(nodePath = x))
    opt[String]("input")
            .required()
            .text("Input edge file path: empty")
            .action((x, c) => c.copy(input = x))
    opt[String]("output")
            .required()
            .text("Output path: empty")
            .action((x, c) => c.copy(output = x))
    opt[String]("cmd")
            .required()
            .text(s"command: ${defaultParams.cmd.toString}")
            .action((x, c) => c.copy(cmd = Command.withName(x)))
    opt[String]("version")
            .text(s"version: ${defaultParams.cmd.toString}")
            .action((x, c) => c.copy(version = Version.withName(x)))

    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class com.nhn.sunny.vegapunk.ml.model.Node2vec \
      """.stripMargin +
              s"|   --lr ${defaultParams.lr}" +
              s"|   --iter ${defaultParams.iter}" +
              s"|   --numPartition ${defaultParams.numPartition}" +
              s"|   --dim ${defaultParams.dim}" +
              s"|   --window ${defaultParams.window}" +
              s"|   --input <path>" +
              s"|   --node <nodeFilePath>" +
              s"|   --output <path>"
    )
  }
  
  def main(args: Array[String]):Unit = {
    TimeRecorder.init()

    parser.parse(args, defaultParams).map { param: Params =>
      val conf: SparkConf = new SparkConf().setAppName("com.navercorp.Node2Vec")
      val context: SparkContext = new SparkContext(conf)
      // 设置log级别
      context.setLogLevel("WARN")

      val N2V: Node2Vec = param.version match {
        case Version.baseline => N2VBaseline
        case Version.partition => N2VPartition
        case Version.broadcast => N2VBroadcast
        case Version.join2 => N2VJoin2
      }

      N2V.setup(context, param)

      param.cmd match {
        case Command.node2vec => N2V.load()
                                         .initTransitionProb()
                                         .randomWalk()
                                         .embedding()
//                                         .save()
        case Command.randomwalk => N2V.load()
                                           .initTransitionProb()
                                           .randomWalk()
//                                           .saveRandomPath()
        case Command.embedding => {
          val randomPaths = Word2vec.setup(context, param).read(param.input)
          Word2vec.fit(randomPaths).save(param.output)
          N2V.loadNode2Id(param.nodePath).saveVectors()
        }
      }
    } getOrElse {
      sys.exit(1)
    }

//     TimeRecorder.show()
//     TimeRecorder.save("~/Projects/node2vec-spark/log/test.log")
  }
}
