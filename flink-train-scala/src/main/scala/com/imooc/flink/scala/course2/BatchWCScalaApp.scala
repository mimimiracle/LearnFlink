package com.imooc.flink.scala.course2

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///D:\\JavaProject\\Flink\\word.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input);

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0).
      sum(1).print()

  }
}
