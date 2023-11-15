

package KafkaFunctionality
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.{File, OutputStreamWriter}
import okhttp3.{OkHttpClient, Request}
import okio.Okio

object SendDataToKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()

    val client = new OkHttpClient()

    // Specify the city, date, and language
    // cities: london, liverpool, manchester, edinburgh, birmingham
    val city = "liverpool"
    val date = "2023-11-07"
    val lang = "en"
    val j_date = "08_nov"

    val endpoint = buildEndpoint(city, date, lang)

    // Add headers to the request
    val request = new Request.Builder()
      .url(s"https://weatherapi-com.p.rapidapi.com/$endpoint")
      .get()
      .addHeader("X-RapidAPI-Key", "757121081cmsh825958e8ab97809p1d38fejsnd2789514b9be")
      .addHeader("X-RapidAPI-Host", "weatherapi-com.p.rapidapi.com")
      .build()

    val response = client.newCall(request).execute()
    val messageDF: DataFrame = if (response.isSuccessful) {
      val responseBody = response.body().string()
      val filePath = s"/tmp/bduk1710/Levina/$city/${city}_$j_date.json"
      //      saveStringAsJsonFile(responseBody, filePath)
      saveStringAsJsonFileToHDFS(spark, responseBody, filePath)

      println(s"API data for $city saved as JSON")
      // Create a DataFrame with the file path
      println(filePath)
      val filePathSeq = Seq((filePath))
      println("48")
      import spark.implicits._
      //      println("50")
      filePathSeq.toDF("File")
//      println("52")
    } else {
      println(s"API request for $city was not successful. Response code: ${response.code()}")
      println(s"Response body: ${response.body().string()}")
      val emptySeq = Seq(("", "No File"))
      import spark.implicits._
      emptySeq.toDF("File")
    }
    println("60")
    val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
    val topicSampleName: String = "weather_forecast"
    println("63")
    messageDF
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", topicSampleName)
      .save()

    println("Message is loaded to Kafka topic")
    Thread.sleep(10000) // Wait for 10 seconds before making the next call

    spark.stop()
  }


  //Function to build the api endpoint URL
  def buildEndpoint(city: String, date: String, lang: String): String = {
    s"history.json?q=$city&dt=$date&lang=$lang"
  }

  // Function to save a string as JSON in a file
  def saveStringAsJsonFile(data: String, filePath: String): Unit = {
    val file = new File(filePath)
    val sink = Okio.buffer(Okio.sink(file))
    sink.writeUtf8(data)
    sink.close()
  }

  // Function to save a string as JSON in a file in HDFS
  def saveStringAsJsonFileToHDFS(spark: SparkSession, data: String, filePath: String): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)

    val outputStream = hdfs.create(new Path(filePath))
    val writer = new OutputStreamWriter(outputStream)

    try {
      writer.write(data)
      println(s"API data saved to HDFS at: $filePath")
    } catch {
      case e: Exception =>
        println(s"Error saving data to HDFS: ${e.getMessage}")
    } finally {
      writer.close()
      hdfs.close()
    }
  }


}