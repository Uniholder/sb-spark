import org.apache.spark.sql.functions._

object users_items extends App with SparkSupport {

  import spark.implicits._

  // extract
  val newData = spark
    .read
    .json(input_dir)
    .drop("_event_type", "_date")
  newData.cache

  // transform
  val latest_date = newData
    .agg(max($"date"))
    .first
    .getString(0)

  val user_items_matrix_new = newData
    .withColumn(
      "event_type_item_id", regexp_replace(
        concat($"event_type", lit("_"), lower($"item_id")), lit("-| "), lit("_")
      )
    )
    .groupBy($"uid")
    .pivot($"event_type_item_id")
    .agg(count($"event_type_item_id"))
    .na.fill(0)

  // load
    if (update == "0") {
      user_items_matrix_new
      .write
      .mode("overwrite")
      .parquet(output_dir + '/' + latest_date)

    newData.unpersist
  } else {
    val latestDir = getLatestPath(output_dir)

    val user_items_matrix_old = spark
      .read
      .parquet(latestDir)

    val user_items_matrix_updated = user_items_matrix_old
      .union(user_items_matrix_new)

      user_items_matrix_updated
      .write
      .mode("overwrite")
      .parquet(output_dir + '/' + latest_date)
  }
}
