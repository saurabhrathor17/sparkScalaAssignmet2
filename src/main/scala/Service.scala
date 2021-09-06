import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object Service {

  /*readInDF will read data from source textFile and it will load in dataset
  * */
  def readInDF(path: String)(implicit spark: SparkSession): Dataset[String] = {
    val load = spark.read.textFile(path)
    load
  }

  // regex ([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)

  /* 1- reading file
  * */
  def cleanFirstDF(readINDF: Dataset[String])(implicit spak: SparkSession): DataFrame = {

    val cleanedData = readINDF
      .withColumn("loggingLevel", functions.split(col("value"), ",").getItem(0))
      .withColumn("timestamp", regexp_extract(col("value"), "([^\\s]+)\\+00:00", 1))
      .withColumn("downloaderId", regexp_extract(col("value"), "ghtorrent-([^\\s]+)", 1).cast("Int"))
      .withColumn("retrivalStage", regexp_extract(col("value"), "([^\\s]+).rb:", 1))
      .withColumn("actualData", regexp_extract(col("value"), "(.*$)", 1))
      .drop("value")
    cleanedData.cache()
  }

  /*2. How many lines does the RDD contain?
  */
  def countRows(cleandFirstDF: DataFrame): Long = {
    val counting = cleandFirstDF.count
    counting
  }

  // 3.Count the number of WARNing messages

  def warningsCounter(cleanedFirstDF: DataFrame): Long = {
    val warnigMessage = cleanedFirstDF.filter(col("loggingLevel") === "WARN").count()
    warnigMessage
  }
  // 4.How many repositories where processed in total? Use the api_client lines only.

  def totalProcessRepo(cleanedFirstDF: DataFrame): Long = {
    val repositories = cleanedFirstDF
      .filter(col("retrivalStage") === "api_client")
      .distinct
      .count
    repositories
  }

  // 5. which client did the most http request

  def mostHttp(cleanedFirstDF: DataFrame): Int = {

    val client = cleanedFirstDF.select("retrivalStage", "downloaderId")
      .filter(col("retrivalStage") === "api_client")
      .groupBy("downloaderId").count()
      .sort(col("count").desc).toDF()
      .first().getInt(0)
    client
  }

  // 6. Which client did most FAILED HTTP requests? Use group_by to provide an answer.
  def mostFailedRequest(cleanedFirstDF: DataFrame): Int = {
    val request = cleanedFirstDF.select("retrivalStage", "actualData", "downloaderId")
      .filter(col("retrivalStage") === "api_client")
      .filter(col("loggingLevel") === "WARN")
      .groupBy("downloaderId").count()
      .sort(col("count").desc).first().getInt(0)
    request
  }

  //7. What is the most active hour of day.

  def activeHour(cleanedFirstDF: DataFrame): String = {
    // val hour = cleanedFirstDF.cast(unix_timestamp(col("timestamp"),"")))
    val newDF = cleanedFirstDF
      .withColumn("time", split(col("timeStamp"), "T").getItem(1))
      .groupBy(col("time")).count().sort(col("count").desc).first().getString(0)
    newDF
  }

  // 8. What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?

  def mostActiveRepo(cleanedFirstDF: DataFrame):String = {

    val repository = cleanedFirstDF
      .filter(col("retrivalStage") === "api_client")
      .withColumn("repos1", split(col("actualData"), "/").getItem(4))
      .withColumn("repos2", split(col("actualData"), "/").getItem(5))
      .select(concat(col("repos1"), lit("/"), col("repos2")).as("repo"))
      .na.drop()
      .groupBy(col("repo"))
      .count().sort(col("count").desc)
      .first().getString(0)
    repository


  }
}