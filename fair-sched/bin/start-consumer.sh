#!/bin/bash

SPARK_HOME=/Users/t3nq/Projects/lib/spark-2.4.5-bin-hadoop2.7

$SPARK_HOME/bin/spark-submit \
	--class com.newprolab.example.StreamConsumer \
	--properties-file /Users/t3nq/Projects/smz/de-spark-scala/fair-sched/resources/spark-properties.conf \
	/Users/t3nq/Projects/smz/de-spark-scala/fair-sched/target/scala-2.11/fair-sched_2.11-0.1.jar \
	localhost:9092
