#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit --class "Bacon" --master local[*] --driver-memory 32g ./target/scala-2.11/bacon_2.11-1.0.jar $1 $2 $3
