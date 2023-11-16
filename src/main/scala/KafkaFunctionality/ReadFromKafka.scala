package KafkaFunctionality
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.commons.mail._
object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "weather_forecast"

    // Define your schema
    val schema = StructType(Seq(
      StructField("wind_mph", DoubleType, nullable = true),
      StructField("humidity", IntegerType, nullable = true)
    ))

    // Define the schema for the JSON messages
//    val schema = StructType(Seq(
//      StructField("id", StringType, nullable = true),
//      StructField("stationName", StringType, nullable = true),
//      StructField("lineName", StringType, nullable = true),
//      StructField("towards", StringType, nullable = true),
//      StructField("expectedArrival", StringType, nullable = true),
//      StructField("vehicleId", StringType, nullable = true),
//      StructField("platformName", StringType, nullable = true),
//      StructField("direction", StringType, nullable = true),
//      StructField("destinationName", StringType, nullable = true),
//      StructField("timestamp", StringType, nullable = true),
//      StructField("timeToStation", StringType, nullable = true),
//      StructField("currentLocation", StringType, nullable = true),
//      StructField("timeToLive", StringType, nullable = true)
//    ))
//    import spark.implicits._
    // Read the JSON messages from Kafka as a DataFrame
//    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")
//    println(df)

    // Read from Kafka and parse JSON data
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .selectExpr("data.*")

    // Show DataFrame
    df.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()

    // Check if the last message's wind is higher than 4
    val lastMessageWind = df.select("wind_mph").orderBy(desc("timestamp")).limit(1).first().getDouble(0)
    if (lastMessageWind > 4.0) {
      // Send an email alert
      sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", s"Last message wind: $lastMessageWind")
    }

    // Start the streaming query
    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()


    // Write the DataFrame as CSV files to HDFS
//    df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/trainarrival/checkpoint").option("path", "/tmp/jenkins/kafka/trainarrival/data").start().awaitTermination()

  }

  // Function to send email alert
  def sendEmailAlert(recipient: String, subject: String, body: String): Unit = {
    val email = new SimpleEmail()
    email.setHostName("smtp.gmail.com")
    email.setSmtpPort(587)
    email.setAuthenticator(new DefaultAuthenticator("15mscit026@gmail.com", "Local$16S"))
    email.setSSLOnConnect(true)
    email.setFrom("15mscit026@gmail.com")
    email.setSubject(subject)
    email.setMsg(body)
    email.addTo(recipient)
    email.send()
  }

}