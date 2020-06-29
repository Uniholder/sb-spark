import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.Pipeline

object train extends App with SparkSupport {

  val trainDataPath = spark.conf.get("spark.mlproject.train_data_path")
  val modelPath = spark.conf.get("spark.mlproject.model_path")

  import spark.implicits._

  val trainData = spark
    .read
    .json(trainDataPath)
    .withColumn("visit", explode($"visits"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .groupBy($"uid", $"gender_age")
    .agg(collect_list($"domain").alias("domains"))

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")
//    .fit(data)

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")
    .fit(trainData)

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(indexer.labels)

  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, labelConverter))

  val model = pipeline.fit(trainData)

  model.write.overwrite().save(modelPath)
}
