import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession


object dashboard extends App {

  val spark = SparkSession
    .builder()
//    .config(sc)
    .getOrCreate()

  import spark.implicits._

  val testData = spark
    .read
    .json("/labs/laba08")
    .withColumn("visit", explode($"visits"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .groupBy($"uid", $"date")
    .agg(collect_list($"domain").alias("domains"))

  val modelLoaded = PipelineModel.load("mlproject")

  val predictions = modelLoaded
    .transform(testData)
    .withColumnRenamed("predictedLabel", "gender_age")
    .select("uid", "gender_age", "date")

  val esOptions =
    Map(
      "es.nodes" -> "10.0.1.9:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> "vladislav.leonov",
      "es.net.http.auth.pass" -> "pass"
    )

  predictions
    .write
    .format("es")
    .options(esOptions)
    .save("vladislav_leonov_lab08/_doc")
}
