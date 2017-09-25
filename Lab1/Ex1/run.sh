$SPARK_HOME/bin/spark-submit --class "WordFreqCounts" --master local[*] target/scala-2.11/WordFreqCounts-assembly-0.1-SNAPSHOT.jar $1
