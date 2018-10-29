package com.pt.spark

import org.apache.spark.sql.SparkSession

/**
  * ./bin/spark-submit --class com.pt.spark.WordCount \
     --master yarn \
     --deploy-mode cluster \
     --executor-cores 2 \
     --queue default \
     /home/work/hadoop-ecological-1.0-SNAPSHOT.jar \
     /user/panteng/LICENSE.txt /user/panteng/word
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("wordCount")
                .getOrCreate()

        val sc = spark.sparkContext

        sc.textFile(args(0))
                .flatMap {
                    line =>
                        line.split(" ")
                }
                .map {
                    word =>
                        (word, 1L)
                }
                .reduceByKey(_ + _)
                .repartition(1)
                .saveAsTextFile(args(1))

        spark.stop()
    }
}
