package com.newprolab.example

import org.apache.spark.sql.SparkSession

trait SparkSupport {

  val spark = SparkSession.builder().getOrCreate()
}
