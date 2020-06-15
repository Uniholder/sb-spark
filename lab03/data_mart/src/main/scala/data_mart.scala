import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source.fromFile

object data_mart extends App {
  val spark = SparkSession
    .builder()
    .appName("data_mart")
    //.config("spark.jars", "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-20_2.11/7.7.1/elasticsearch-spark-20_2.11-7.7.1.jar")
    //.config("spark.jars", "https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.4.3/spark-cassandra-connector_2.11-2.4.3.jar")
    //.config("spark.jars", "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar")
    //.config("spark.jars.packages","org.elasticsearch:elasticsearch-spark-20_2.11:7.7.1")
    .getOrCreate()

  spark.conf.set("spark.locality.wait", "0")

  // Информация о клиентах
  //spark.sparkContext.addJar("spark-cassandra-connector_2.11-2.4.3.jar")
  spark.conf.set("spark.cassandra.connection.host", "10.0.1.9")
  val tableOpts = Map("table" -> "clients","keyspace" -> "labdata")

  val clientsDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load()
    .withColumn("age_cat",
      when(col("age") >= 18 && col("age") <= 24, "18-24")
        .when(col("age") >= 25 && col("age") <= 34, "25-34")
        .when(col("age") >= 35 && col("age") <= 44, "35-44")
        .when(col("age") >= 45 && col("age") <= 54, "45-54")
        .when(col("age") >= 55, ">=55")
    )

  // Логи посещения интернет-магазина
  //spark.sparkContext.addJar("elasticsearch-spark-20_2.11-7.7.1.jar")
  val esOptions =
    Map(
      "es.nodes" -> "10.0.1.9:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )
  val shopLogsDF = spark
    .read
    .format("es")
    .options(esOptions)
    .load("visits")
    //.repartition(9)
    .na.drop(Seq("uid"))
    .withColumn(
      "shop_category", regexp_replace(
        concat(lit("shop_"), lower(col("category"))), lit("-| ") , lit("_")
      )
    )

  // Логи посещения веб-сайтов
  val webLogDF = spark
    .read
    .parquet("hdfs:///labs/laba03/weblogs.parquet")
    .withColumn("visit", explode(col("visits")))
    .withColumn("domain", callUDF("parse_url", col("visit.url"), lit("HOST")))
    .withColumn(
      "domain", regexp_replace(col("domain"), lit("www\\."), lit(""))
    )

  // Информация о категориях веб-сайтов
  //spark.sparkContext.addJar("postgresql-42.2.14.jar")
  val password = fromFile("conf.password").getLines.toList(0)
  val jdbcUrl_domain_cats = "jdbc:postgresql://10.0.1.9:5432/labdata?user=vladislav_leonov&password=" + password
  println(jdbcUrl_domain_cats)

  val siteCatsDF = spark
    .read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", jdbcUrl_domain_cats)
    .option("dbtable", "domain_cats")
    .load()
    .withColumn(
      "web_category", regexp_replace(
        concat(lit("web_"), lower(col("category"))), lit("-| ") , lit("_")
      )
    )
    .repartition(col("web_category"))

  // Витрина
  val webLogDF_cat = webLogDF
    .join(broadcast(siteCatsDF), Seq("domain"), "full")
    .groupBy(col("uid"))
    .pivot(col("web_category"))
    .agg(count(col("web_category")))

  val data_mart = clientsDF
    .select(col("uid"), col("gender"), col("age_cat"))
    .join(shopLogsDF, Seq("uid"), "left")
    .groupBy(col("uid"), col("gender"), col("age_cat"))
    .pivot(col("shop_category"))
    .agg(count(col("shop_category")))
    .join(broadcast(webLogDF_cat), Seq("uid"), "inner")
    .drop("null")

  val jdbcUrl = "jdbc:postgresql://10.0.1.9:5432/vladislav_leonov?user=vladislav_leonov&password=" + password

  data_mart
    .write
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "clients")
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save

  spark.stop
}