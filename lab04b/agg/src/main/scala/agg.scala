import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object agg extends App with SparkSupport {

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val kafkaParamsInput = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "subscribe" -> "vladislav_leonov",
    "startingOffsets" -> """ { "vladislav_leonov": { "0": -2 } } """
  )

  val kafkaParamsOutput = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "topic" -> "vladislav_leonov_lab04b_out"
  )

  val schemaShop = new StructType()
    .add("category", StringType)
    .add("event_type", StringType)
    .add("item_id", StringType)
    .add("item_price", LongType)
    .add("timestamp", StringType)
    .add("uid", StringType)

  val sdfShop = spark
    .readStream
    .format("kafka")
    .options(kafkaParamsInput)
    .load
    .select(from_json(col("value").cast("string"), schemaShop).as("data"))
    .select("data.*")
    .withColumn("ts", to_timestamp(col("timestamp") / 1000))

  val result = sdfShop
    .withWatermark("ts", "10 minutes")
    .groupBy(window(col("ts"), "1 hour", "1 hour"))
    .agg(
      sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).as("revenue"),
      count(col("uid")).as("visitors"),
      count(when(col("event_type") === "buy", col("event_type"))).as("purchases")
    )
    .select(
      to_json(
        struct(unix_timestamp(col("window.start")).alias("start_ts"),
          unix_timestamp(col("window.end")).alias("end_ts"),
          col("revenue"),
          col("visitors"),
          col("purchases"),
          (col("revenue") / col("purchases")).alias("aov"))
      ).as("value")
    )

  val sink = result
    .writeStream
    .outputMode("update")
    .format("kafka")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .options(kafkaParamsOutput)
    .option("checkpointLocation", s"chk/lab04b")
    .start()
    .awaitTermination()
}
