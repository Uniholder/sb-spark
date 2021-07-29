package com.newprolab.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.Trigger


//case class Airport(ident: String, elevation_ft: Int, timestamp: java.sql.Timestamp, iso_country: String)


object StreamConsumer extends App with SparkSupport {

  import spark.implicits._

  //  val schema = ScalaReflection.schemaFor[Airport].dataType.asInstanceOf[StructType]
  //  val schema = spark.emptyDataset[Airport].schema

  def batchLogic(batchDf: DataFrame, batchId: Long): Unit = {

    val usFunc = (batchDf: DataFrame) =>
      batchDf.filter(col("iso_country") === "US")
        .write.mode("append").parquet("../data/3.parquet")

    val batchShow = (batchDf: DataFrame) => batchDf.show(20, false)

    val batchCount = (batchDf: DataFrame) => println(batchDf.count)

    val groupedBy = (batchDf: DataFrame) => batchDf.groupBy(col("iso_country")).count().show(30, false)

    val usDfWrite = (batchDf: DataFrame) => batchDf.write.mode("append").parquet("../data/1.parquet")
//    usDf.write.mode("append").parquet("../data/1.parquet")
//    ruDf.write.mode("append").orc("../data/2.orc")
//    caDf.groupBy(col("ident")).count().write.mode("append").json("../data/3.json")
//    println(chDf.count())

    List(usFunc, batchShow, batchCount, groupedBy, usDfWrite).par.foreach { x => x(batchDf) }

  }

  override def main(args: Array[String]): Unit = {

    val schema = StructType(
      StructField("ident", StringType) ::
        StructField("elevation_ft", IntegerType) ::
        StructField("timestamp", TimestampType) ::
        StructField("iso_country", StringType) :: Nil
    )

    val kafkaAddr = args.toList match {
      case kA :: Nil => kA
      case _ => throw new IllegalArgumentException("Must specify kafka address and dataset path")
    }

    println(schema)

    val df = spark
      .readStream.format("kafka")
      .option("subscribe", "test_topic0")
      .option("kafka.bootstrap.servers", kafkaAddr)
      .option("maxOffsetsPerTrigger", "10000")
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value").cast("string").alias("value"))
      .select(from_json(col("value"), schema).alias("value"))
      .select(col("value.*"))

    df.writeStream
      .foreachBatch((batchDf, batchId) => batchLogic(batchDf, batchId))
      .option("checkpointLocation", "../chk/2")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

  }
}
