import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filter extends App{
  val sc = new SparkConf()

  val spark = SparkSession
    .builder()
    .config(sc)
    .appName("filter")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  import spark.implicits._

  def offset(x: Any): Any = x match {
    case "earliest" => -2
    case _ => x
  }

  val eventsDf = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.1.13:6667")
    .option("subscribe", spark.conf.get("spark.filter.topic_name"))
    .option("startingOffsets", f""" { "${spark.conf.get("spark.filter.topic_name")}": { "0": ${offset(spark.conf.get("spark.filter.offset"))} } } """)
    .load

  val eventsJsonStrings = eventsDf.select(col("value").cast("string")).as[String]

  val eventEnriched = spark
    .read
    .json(eventsJsonStrings)
    .withColumn(
      "date", date_format(
        to_date(
          from_unixtime(col("timestamp") / 1000)
        ), "YYYYMMDD"
      )
    )
    .withColumn("_date", col("date"))
    .withColumn("_event_type", col("event_type"))

  eventEnriched
    .write
    .mode("overwrite")
    .partitionBy("_event_type", "_date")
    .json(f"${spark.conf.get("spark.filter.output_dir_prefix")}")
}
