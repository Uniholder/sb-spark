import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.SparkSession

object features extends App{

  val spark = SparkSession.builder().getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  import spark.implicits._

  // get user-items matrix
  val userItemsMatrix = spark
    .read
    .parquet("/user/vladislav.leonov/users-items/20200429")

  // get web logs
  val webLogDF = spark
    .read
    .parquet("hdfs:///labs/laba03/weblogs.parquet")
    .withColumn("visit", explode($"visits"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .withColumn("ts", to_timestamp($"visit.timestamp" / 1000))

  // create day-of-week matrix
  val userDayOfWeekMatrix = webLogDF
    .withColumn(
      "web_day_of_week", concat(
        lit("web_day_"), lower(
          date_format($"ts", "E")
        )
      )
    )
    .groupBy($"uid")
    .pivot($"web_day_of_week")
    .agg(count($"web_day_of_week"))

  // create hour matrix
  val userHourMatrix = webLogDF
    .withColumn(
      "web_hour_n", concat(
        lit("web_hour_"), hour($"ts")
      )
    )
    .groupBy($"uid")
    .pivot($"web_hour_n")
    .agg(count($"web_hour_n"))

  // count working and evening hours (matrix style)
  val userWorkEveningCount = webLogDF
    .withColumn("work_evening",
      when(hour($"ts") >= 9 && hour($"ts") < 18, "work_hours")
        .when(hour($"ts") >= 18 && hour($"ts") < 24, "evening_hours")
    )
    .groupBy($"uid")
    .pivot($"work_evening")
    .count
    .drop($"null")

  // create hours-fraction matrix
  val userHoursFractionMatrix = webLogDF
    .groupBy($"uid")
    .count()
    .join(userWorkEveningCount, Seq("uid"), "inner")
    .withColumn("web_fraction_work_hours", $"work_hours" / $"count")
    .withColumn("web_fraction_evening_hours", $"evening_hours" / $"count")
    .select("uid", "web_fraction_work_hours", "web_fraction_evening_hours")

  // collect domains by uid
  val userDomains = webLogDF
    .groupBy($"uid")
    .agg(collect_list($"domain").alias("domains"))

  // array of top-1000 by frequency domains (alphabetic order)
  val domainArray = webLogDF
    .filter($"domain" =!= "null")
    .groupBy($"domain")
    .count
    .orderBy($"count".desc)
    .limit(1000)
    .orderBy($"domain")
    .select("domain")
    .as[String]
    .collect

  // define CountVectorizerModel with a-priori vocabulary
  val cvm = new CountVectorizerModel(domainArray)
    .setInputCol("domains")
    .setOutputCol("domain_features_sparse")

  // tranform domains to sparse vector
  val userDomainsWithSparseVector = cvm.transform(userDomains)

  val asDense = udf((v: SparseVector) => v.toDense)
  val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray // for checker
  val toArrUdf = udf(toArr) // for checker

  // sparse vector to dense vector
  val userDomainsWithDenseVector = userDomainsWithSparseVector
    .withColumn("domain_features", toArrUdf(asDense($"domain_features_sparse")))
    .select("uid", "domain_features")

  // join all matrices
  val features = userDayOfWeekMatrix
    .join(broadcast(userHourMatrix), Seq("uid"))
    .join(broadcast(userHoursFractionMatrix), Seq("uid"))
    .join(broadcast(userDomainsWithDenseVector), Seq("uid"))
    .join(broadcast(userItemsMatrix), Seq("uid"), "left")
    .na.fill(0)

  // write to parquet
  features
    .write
    .mode("overwrite")
    .parquet("features")
}
