package com.newprolab.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions._

class MyDatasourceSpec extends FlatSpec with Matchers{
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()

  val schema = StructType(
    StructField("id", LongType) ::
    StructField("foo", StringType) ::
    Nil
  )

  val df = spark
    .readStream
    .format("org.apache.spark.sql.npl.myDatasourceProvider")
    .option("step", "10")
    .schema(schema)
    .load()
    .groupBy(col("foo"))
    .count

  df
    .writeStream
    .outputMode("complete")
    .options(Map("lalala" -> "foofoofoo"))
    .partitionBy("foo")
//    .format("console")
    .format("org.apache.spark.sql.npl.MyDatasinkProvider")
    .option("checkpointLocation", "path")
    .trigger(Trigger.ProcessingTime(5000))
    .start()
    .awaitTermination(20000)


//  val foo = 2
//  "foo" should "be 2" in {
//    foo shouldBe 3
//  }
//  it should "be more than 1" in {
//    foo should be > 1
//  }
//  "Bar" should "be not like foo" in {
//
//  }
}
