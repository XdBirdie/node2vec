package edu.cuhk.rain.util

import scopt.OptionParser


case object ParamsPaser {
  val defaultParams: Params = Params()
  val parser: OptionParser[Params] = new OptionParser[Params]("node2vec") {
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

    opt[Int]("dim")
      .text(s"dim: ${defaultParams.dim}")
      .action((x: Int, c: Params) => c.copy(dim = x))

    opt[Boolean]("weighted")
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x: Boolean, c: Params) => c.copy(weighted = x))

    opt[Boolean]("directed")
      .text(s"directed: ${defaultParams.directed}")
      .action((x: Boolean, c: Params) => c.copy(directed = x))

    opt[Boolean]("indexed")
      .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
      .action((x: Boolean, c: Params) => c.copy(indexed = x))

    opt[String]("nodePath")
      .text("Input node2index file path: empty")
      .action((x: String, c: Params) => c.copy(nodePath = x))

    opt[Int]("window")
      .text(s"window: ${defaultParams.window}")
      .action((x: Int, c: Params) => c.copy(window = x))

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

    opt[Int]("partitions")
      .text(s"partitions: ${defaultParams.partitions}")
      .action((x: Int, c: Params) => c.copy(partitions = x))
  }

  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding, linkPredict, partition = Value
  }

  object Version extends Enumeration {
    type Version = Value
    val baseline, partition, broadcast, join2, one = Value
  }

  import Command._

  def parse(args: Array[String]): Option[Params] = {
    parser.parse(args, defaultParams)
  }

  case class Params( // node2vec参数
                     iter: Int = 10,
                     lr: Double = 0.025,
                     dim: Int = 128,
                     window: Int = 10,
                     minCount: Int = 0,
                     // rw参数
                     walkLength: Int = 80,
                     numWalks: Int = 10,
                     p: Double = 1.0,
                     q: Double = 1.0,
                     // graph参数
                     weighted: Boolean = true,
                     directed: Boolean = false,
                     indexed: Boolean = true,
                     // 输入/输出参数
                     nodePath: String = null,
                     input: String = null,
                     output: String = null,

                     partitions: Int = 10,
                     cmd: Command = Command.node2vec
                   ) extends Serializable

}
