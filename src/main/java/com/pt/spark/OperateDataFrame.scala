package com.pt.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame是DataSet的特例,每一行记录类型是固定的Row() 定义：type DataFrame = Dataset[Row]
  */
object OperateDataFrame {

    case class MyString(pre: String, value: String)

    case class StringWrapper(myString: MyString)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("wordCount")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        val df = spark.read.text("README.md")
        df.printSchema()
        df.show()

        val myStrSet = df
                .filter(row => row.getAs[String](0).startsWith("#"))
                .map {
                    row =>
                        val array = row.getAs[String](0).split(" ")
                        StringWrapper(MyString(array(0), array(1)))
                }
                .map {
                    stringWrapper =>
                        stringWrapper.myString
                }

        myStrSet.printSchema()
        myStrSet.show()
        myStrSet.write.json("/tmp/spark_ds.json")
    }
}
