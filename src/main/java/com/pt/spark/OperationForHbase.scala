package com.pt.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapred.JobConf

object OperationForHbase {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .appName("operationForHbase")
                .master("local[2]")
                .getOrCreate()
        val sc = spark.sparkContext
        val scan = new Scan()
                .addFamily(Bytes.toBytes("cf1"))
                .withStartRow(Bytes.toBytes("a"))
                .withStopRow(Bytes.toBytes("z"))
        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, "test")
        conf.set(TableInputFormat.SCAN, convertScanToString(scan))
        conf.set("hbase.zookeeper.quorum", "10.38.161.138")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.unsafe.stream.capability.enforce", "false")
        conf.set("hbase.cluster.distributed", "true")

        //读
        val hbaseRdd = sc.newAPIHadoopRDD(conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]).map {
            case (rowkeyBytes, result) =>
                (Bytes.toString(rowkeyBytes.get()), Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("a"))))
        }
        hbaseRdd.collect().foreach(println)


        //写
        conf.set(TableOutputFormat.OUTPUT_TABLE, "test")
        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])

        sc.parallelize(
            Seq(("row3", "中文"), ("row4", "但d地"),
                ("row5", "de123"), ("row6", "dou"))
        ).map {
            case (rowKey, value) =>
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("a"), Bytes.toBytes(value))
                (new ImmutableBytesWritable, put)
        }.saveAsHadoopDataset(jobConf) //此处使用newApi会报错

        spark.stop()
    }

    def convertScanToString(scan: Scan): String = {
        val proto = ProtobufUtil.toScan(scan)
        Base64.encodeBytes(proto.toByteArray)
    }
}
