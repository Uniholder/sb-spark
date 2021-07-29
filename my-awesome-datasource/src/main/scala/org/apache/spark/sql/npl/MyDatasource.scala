package org.apache.spark.sql.npl

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MyDatasourceProvider extends StreamSourceProvider with Logging{
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    logInfo("sourceSchema call")
    val first = "test"
    schema match {
      case Some(x) => (first, x)
      case None => throw new IllegalArgumentException("Schema A must be set")
    }
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    logInfo(metadataPath)
    logInfo(providerName)
    logInfo("createSource call")
    logInfo(schema.toString)

    val step = parameters.get("step") match {
      case Some(x) => x.toInt
      case None => throw new IllegalArgumentException("step must be specified")
    }

    schema match {
      case Some(x) => new MyDatasource(x, step)
      case None => throw new IllegalArgumentException("Schema B must be set")
    }
  }
}

class MyDatasource(dataSchema: StructType, step: Int) extends Source with Logging {

  var i = 0

  override def schema: StructType = {
    logInfo("provider schema call")
    this.dataSchema
  }

  override def getOffset: Option[Offset] = {
    logInfo("provider getOffset call")
//    Option.empty[Offset]
    val currentOffset = new MyOffset(i)
    i += 1
    Some(currentOffset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo("provider getBatch call")
//    SparkSession.active.range(0, 10).toDF()
    logInfo(s"Offset: start=${start}, end=${end}")
    val startingOffset = start match {
      case Some(x) => x.json.toInt * step + 1
      case None => 0
    }
    val endingOffset = end.json.toInt * step

    val spark = SparkSession.active
    val sparkContext = spark.sparkContext
    val catalystRows: RDD[InternalRow] = sparkContext.parallelize(startingOffset to endingOffset).map{x =>
      InternalRow.fromSeq(Seq(x.toLong, UTF8String.fromString("hello world")))
    }
    val isStreaming = true
    val df = spark.internalCreateDataFrame(catalystRows, this.schema, isStreaming)

    df
  }

  override def stop(): Unit = {
    logInfo("provider stop call")
    Unit
  }
}

class MyOffset(value: Int) extends Offset {
  override def json(): String = value.toString
}
