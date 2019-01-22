package com.pt.spark

import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * DataSet是强类型的结构化数据集，支持spark sql操作；
  * 与RDD的主要区别：
  * 1. spark不了解RDD元素的内部结构，只有用户代码可以了解、解析、处理；
  * 但是spark知道dataSet数据的内部结构（包含哪些列，每列的数据类型），因此可以做更多的事情
  * 2. DS数据以编码之后的二进制形式存储（区别与java对象），不需要反序列化就可以进行shuffle&sort等操作；即便需要反序列化的此情况下
  * DS的效率也比RDD高很多
  * 3. 基于spark sql引擎，在内部牺牲了一定的不变性来减少内存的占用，减少GC次数；
  * 4. 引入了schema和off-heap，对内存的管理使用受到JVM的约束会更少
  * 5. 提供了更多的数据保存格式,csv、json、parquet、jdbc、orc等
  *
  * 总之DS比RDD性能要好很多（执行时间、内存占用、序列化速度）
  * 详见：https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html
  */
object OperateDataSet {

    //case class的定义在main函数外面，否则会报异常
    case class Person(name: String, age: Int, isMan: Boolean)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("wordCount")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
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
        }.show(false)

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
        sqlDF.write.json("/tmp/spark_json")

    }
}
