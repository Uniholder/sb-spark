import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object test extends App with SparkSupport {

  val kafkaTopicInput = spark.conf.get("spark.mlproject.kafka_input")
  val kafkaTopicOutput = spark.conf.get("spark.mlproject.kafka_ouput")
  val modelPath = spark.conf.get("spark.mlproject.model_path")

  import spark.implicits._

  val model = PipelineModel.load(modelPath)

  val kafkaParamsInput = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "subscribe" -> f"$kafkaTopicInput",
    "startingOffsets" -> f""" { "$kafkaTopicInput": { "0": -2 } } """
  )

  val kafkaParamsOutput = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "topic" -> f"$kafkaTopicOutput"
  )

  val schema = new StructType()
    .add("uid", StringType)
    .add(
      "visits", ArrayType(
        StructType(
          Array(
            StructField("timestamp", LongType),
            StructField("url", StringType)
          )
        )
      )
    )

  val testData = spark
    .readStream
    .format("kafka")
    .options(kafkaParamsInput)
    .load
    .select(from_json(col("value").cast("string"), schema).as("data"))
    .select("data.*")
    .withColumn("visit", explode($"visits"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .groupBy($"uid")
    .agg(collect_list($"domain").alias("domains"))

  val predictions = model
    .transform(testData)
    .withColumnRenamed("predictedLabel", "gender_age")
    .select(
      to_json(
        struct(
          col("uid"),
          col("gender_age")
        )
      ).as("value")
    )

  val sink = predictions
    .writeStream
    .outputMode("update")
//    .outputMode("complete")
    .format("kafka")
//    .trigger(Trigger.Once())
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .options(kafkaParamsOutput)
    .option("checkpointLocation", s"chk/lab07")
    .start()
    .awaitTermination()
}
