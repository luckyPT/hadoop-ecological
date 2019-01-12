package com.pt.spark

import org.apache.spark.sql.{Encoder, SparkSession}

object OperateDataSet {

    //case class的定义在main函数外面，否则会报异常
    case class Person(name: String, age: Int, isMan: Boolean)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("wordCount")
                .master("local[*]")
                .getOrCreate()
        //可用于提供一些默认的Encoder，但并不是所有的
        import spark.implicits._

        val ds = spark.read.textFile("README.md").persist()
        //输出
        ds.show(numRows = 10, truncate = false)
        ds.printSchema()

        //找字符最多的行
        val maxCount = ds.map {
            line =>
                (line, line.split("").length)
        }.reduce {
            (a, b) =>
                if (a._2 > b._2) {
                    a
                } else {
                    b
                }
        }
        println(maxCount)

        //JAVA 对象转DS，需要自己定义Encoder
        case class LineWithId(text: String, id: Int)

        implicit val lineWithIdEncoder: Encoder[LineWithId] = org.apache.spark.sql.Encoders.kryo[LineWithId]
        ds.map {
            line =>
                LineWithId(line, line.hashCode)
        }.map {
            lineWithId =>
                Seq(lineWithId.id, lineWithId.text).mkString("\t")
        }.show()

        //ds的一些基本操作
        import spark.implicits._
        val peoples = Seq(
            Person("anlen", 15, isMan = false),
            Person("sun", 16, isMan = true),
            Person("andy", 17, isMan = false),
            Person("tony", 18, isMan = true),
            Person("robin", 23, isMan = true),
            Person("mack", 26, isMan = true)
        ).toDS().cache()
        peoples.printSchema()
        peoples.show()
        peoples.select("name", "age").show()
        peoples.select($"name", $"age" + 1).show()
        peoples.filter($"age" > 20).show()
        peoples.groupBy("isMan").count().show()

        peoples.createOrReplaceTempView("people")
        //常见的SQL函数基本都支持
        val sqlDF = spark.sql("SELECT avg(age) FROM people")
        sqlDF.show()

    }
}
