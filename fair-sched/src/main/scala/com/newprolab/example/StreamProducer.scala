package com.newprolab.example

import sys.process._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamProducer extends App with SparkSupport {

  def batchLogic(df: DataFrame, id: Long): Unit = {
    df.show
  }

  override def main(args: Array[String]): Unit = {
    val (kafkaAddr, datasetPath) = args.toList match {
      case kA :: dP :: Nil => (kA, dP)
      case _ => throw new IllegalArgumentException("Must specify kafka address and dataset path")
    }

    val airportsDf =
      spark
        .read
        .format("csv")
        .options(Map("inferSchema" -> "true", "header" -> "true"))
        .load(datasetPath)

    val rateStreamDf = spark.readStream.format("rate").load()
    val joined = rateStreamDf.crossJoin(airportsDf.sample(0.1))

    //    val sq = joined.writeStream.foreachBatch((batchDf, batchId) => batchLogic(batchDf, batchId)).start
    val sq =
      joined
        .toJSON
        .withColumn("topic", lit("test_topic0"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafkaAddr)
        .option("checkpointLocation", "../chk/1").start

    while (true) {
      sq.awaitTermination(10000)
      println(sq.status)
      println(sq.lastProgress)
      val stopMarker = "ls -alh /Users/t3nq/Projects/smz/de-spark-scala/fair-sched/mgmt".!!
      if (stopMarker contains "stream.stop") {
        sq.stop
        sys.exit(0)
      }
      else
        println("go ahead")
    }


  }
}
