package com.ys.streaming.profile

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/11/14.
  */
object RDDTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConf.set("spark.hadoop.mapred.output.compress", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    sparkConf.set("spark.core.connection.ack.wait.timeout","120")
    sparkConf.set("spark.akka.timeout","180")
    sparkConf.set("spark.akka.logLifecycleEvents", "false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val iteblog1= sc.parallelize(List(1,2,3))
    iteblog1.map(one => ((1,2),one.toString)).reduceByKey(_ + "" + _)
  }
}
