package com.pt.flink

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * DataStream - 运行模型
 * 主要由：source、transformation、sink三部分组成
 * source：file、socket、fromCollection、fromElements、generateSequence、自定义数据源addSource 如：kafka、mysql
 * sink：writeAsText、WriteAsCsv、print、writeToSocket、writeUsingOutputFormat、自定义sink addSink 如：kafka等
 * transformation算子：map、flatMap、filter、keyBy、reduce、fold、aggregations、connect、coMap、coFlatMap、split、select、union
 * 程序编写流程：
 * 1. 获取执行环境
 * 2. 接入数据源
 * 3. 开发数据处理、转换逻辑
 * 4. 指定存放位置
 * 5. 触发程序执行
 * web ui：http://localhost:8081/
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    println("----start flink----")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration)
    val filePath = "pom.xml"
    val text = env.readFile(new TextInputFormat(new Path(filePath)), filePath,
      FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
    text
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1L))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("FirstExample")
  }
}
