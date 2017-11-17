package com.ys.streaming.framework

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by yangshuo on 2017/11/11.
  * Class extending this abstract class are implement userdefine function
  */
abstract class StreamingFramework(val name : String,
                                  val batchDuration :Duration) extends Serializable {

  Logger.getLogger("").setLevel(Level.WARN)

  /*
  * userdefine function
  */
  def userdefined(streamingContext: StreamingContext)

  def run(args : Array[String]): Unit = {
    usage(args)
    // define the method, get StreamingContext
    def getStreamingContext(): StreamingContext = {
      val sparkConf = new SparkConf().setAppName(name)
        .set("spark.streaming.receiver.writeAheadLog.enable", "true")
        .set("spark.hadoop.mapred.output.compress", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
        .set("spark.core.connection.ack.wait.timeout","120")
        .set("spark.akka.timeout","180")
        .set("spark.akka.logLifecycleEvents", "false")
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.streaming.stopGracefullyOnShutdown","true")

      // Stop Gracefully
      val sCtx = new StreamingContext(sparkConf, batchDuration)
      sCtx
    }
    // create new StreamingContext or get from checkpoint
//    val streamingContext  = StreamingContext.getOrCreate("", getStreamingContext)
    val streamingContext  = getStreamingContext()

    // user define process,process the specific business
    userdefined(streamingContext)
    // start streaming context && loop the circulation
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def usage[T](t : T): Unit = {}

}

