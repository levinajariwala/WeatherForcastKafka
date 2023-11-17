package KafkaFunctionality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import javax.mail._
import javax.mail.internet._

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "weather_forecast_kafka"

    val schema = StructType(Seq(
      StructField("wind_mph", DoubleType, nullable = true),
      StructField("localtime", StringType, nullable = true) // Change to StringType as per the data received
    ))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .selectExpr("data.*")
      .withColumn("is_alert", when(col("wind_mph") > 2.0, 1).otherwise(0))

    // Write the DataFrame to Hive table
//    df.writeStream
//      .outputMode("append")
//      .format("hive")
//      .option("checkpointLocation", "/tmp/bduk1710/Levina/wind_info")
////      .option("checkpointLocation", "/path/to/checkpoint") // Specify the checkpoint location
//      .option("table", "bduk_test1. wind_info") // Specify your Hive table name
//      .start()

    df.write
      .format("hive")
      .option("database", "bduk_test1") // Specify the database name
      .mode("overwrite") // Choose the appropriate mode (overwrite, append, etc.)
      .saveAsTable("wind_info")

    import spark.implicits._

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        print("\n\n\n\n\n")
        println("Entire Batch:")
        batchDF.show(truncate = false)
        print("\n\n\n\n\n")
        if (!batchDF.isEmpty) {
          val lastRecordDF = batchDF.orderBy($"localtime".desc).limit(1)
          print("\n\n\n\n\n")
          println("Last Record:")
          lastRecordDF.show(truncate = false)
          print("\n\n\n\n\n")

          if (!lastRecordDF.isEmpty) {
            val windSpeedRow = lastRecordDF.select("wind_mph").collectAsList()

            if (!windSpeedRow.isEmpty) {
              val windSpeed = windSpeedRow.get(0).getAs[Double]("wind_mph")

              if (windSpeed > 2.0) {
                sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", "High wind speed detected!")
                println("High wind speed detected!")
              }
            }
          }
        }
      }
      .start()

    query.awaitTermination()
  }

  def sendEmailAlert(recipient: String, subject: String, body: String): Unit = {
    val properties = new java.util.Properties()
    properties.put("mail.smtp.host", "smtp.gmail.com")
    properties.put("mail.smtp.port", "587")
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getInstance(properties, new javax.mail.Authenticator() {
      override protected def getPasswordAuthentication(): PasswordAuthentication = {
        new PasswordAuthentication("15mscit026@gmail.com", "zvqm ctzt izma xkaa")
      }
    })

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress("15mscit026@gmail.com"))
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient))
      message.setSubject(subject)
      message.setText(body)
      Transport.send(message)
      println("Email sent successfully!")
    } catch {
      case e: MessagingException => e.printStackTrace()
    }
  }
}
