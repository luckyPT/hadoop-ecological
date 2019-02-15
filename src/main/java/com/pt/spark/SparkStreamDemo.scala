package com.pt.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

/**
  * fileStream
  * socketStream
  * kafka
  * flume
  * Kinesis
  */
object SparkStreamDemo {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("wordCount")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
        //fileStream(ssc)
        //socketStream(ssc)
        socketStreamWindow(ssc)

    }

    /**
      * 经测试，在该目录下通过vim新建文件并保存和通过cp命令拷贝过去的文件可以正常监控，但是通过窗口拷贝过去的就不能监控
      *
      * @param ssc StreamingContext
      */
    def fileStream(ssc: StreamingContext): Unit = {
        val lines = ssc.textFileStream("/home/panteng/桌面/stream")
        val wordCount = lines.flatMap {
            str =>
                str.split(" ")
        }.map {
            str =>
                (str, 1L)
        }.reduceByKey(_ + _)
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }

    def socketStream(ssc: StreamingContext): Unit = {
        val lines = ssc.socketTextStream("localhost", 9999)
        //也是按行处理，要求服务端发送数据完毕之后必须加换行符
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
        wordCounts.print()
        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate
    }

    /**
      * DEMO:每隔6秒钟统计最近30秒的数据，每隔1分钟存储一次
      * 应用场景：每天更新用户最近30天的行为数据
      *
      * @param ssc StreamingContext
      */
    def socketStreamWindow(ssc: StreamingContext): Unit = {
        val lines = ssc.socketTextStream("localhost", 9999)
        val statistics = lines.flatMap(_.split(" "))
                .map(w => (w, 1))
                .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(6))
        statistics.print()
        statistics.window(Minutes(1), Minutes(1))
                .repartition(1).saveAsTextFiles("/home/panteng/IdeaProjects/hadoop-ecological/output/time")
        ssc.start()
        ssc.awaitTermination()
    }
}
