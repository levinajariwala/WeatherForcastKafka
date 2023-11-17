package KafkaFunctionality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import org.apache.spark.sql.types._

//import org.apache.commons.mail._
import javax.mail._
import javax.mail.internet._

object ReadFromKafka  {
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
      StructField("localtime", IntegerType, nullable = true)
    ))

    // Read from Kafka and parse JSON data
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .selectExpr("data.*")

    import spark.implicits._

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val lastRecordDF = batchDF.orderBy($"localtime".desc).limit(1)
        print("iiiiiiiiiiiiiiiiiiiiiiiiinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn")
        print(batchDF)
        if (!lastRecordDF.isEmpty) {
          val windSpeedRow = lastRecordDF.select("wind_mph").collectAsList()

          if (!windSpeedRow.isEmpty) {
            val windSpeed = windSpeedRow.get(0).getAs[Double]("wind_mph")
            print("\n\n\n\n")
            print(windSpeed)
            print("\n\n\n\n")
            if (windSpeed > 2.0) {
              sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", "High wind speed detected!")
              print("\n\n\n\n")
              println("High wind speed detected!") // Print alert message
              print("\n\n\n\n")
            }
          }
        }
      }
      .start()


    //    val lastRecordDF = df.orderBy($"localtime".desc).limit(1)
//    println("\n\n\n")
//    println(lastRecordDF)
//    println("\n\n\n")
//    // Check the last record for high wind speed
//    if (!lastRecordDF.isEmpty && lastRecordDF.select("wind_mph").head().getDouble(0) > 5.0) {
//      println("\n\n\n")
//      println("!!!!!!!!!!AAAAALLLLLLEEEEEERRRRRRTTTTTTT!!!!!!!")
//      println("\n\n\n")
//      sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", "High wind speed detected!")
//    }
    // Check if the last message's wind is higher than 4
    // Retrieve the last wind speed from the DataFrame
    // Output the processed data to the console (for demonstration purposes)
//    val query = df.writeStream
//      .outputMode("append")
//      .format("console")
//      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .start()

//    // Fetching the last record and checking wind speed
//    val lastWindSpeedDF = df.select("wind_mph", "localtime")
//      .orderBy(col("localtime").desc)
//      .limit(1)
//
//    val lastWindSpeedRow = lastWindSpeedDF.collect().headOption
//    println("\n\n\n")
//    println(lastWindSpeedRow)
//    println("\n\n\n")


    //    val lastMessageWind = 5.0
//    sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", s"Last message wind: $lastMessageWind")

//    import org.apache.spark.sql.functions.{max, col}
//
//    val lastWindSpeedDF = df.select(max("timestamp").as("max_timestamp"))
//      .join(df, col("timestamp") === col("max_timestamp"))
//      .select("wind_mph")
//
//    val lastWindSpeed = lastWindSpeedDF.collect().map(_.getDouble(0)).headOption
//
//    lastWindSpeed.foreach { windSpeed =>
//      println("Last Wind Speed Retrieved: " + windSpeed)
//
//      if (windSpeed > 4.0) {
//        println("!!!!!!!!!!!!!!!!AAAALLLLEEERRRRTTTT!!!!!!!!!!!!!!!!!!!")
//        println(s"High Wind Speed Detected: $windSpeed mph")
//        // Any action or alert for high wind speed
//      } else {
//        println("No high wind speed detected.")
//      }
//    }
    query.awaitTermination()

    //    val lastWindSpeed = df.select("wind_mph")
//      .orderBy(desc("timestamp"))
//      .limit(1)
//      .first()
//      .getDouble(0)
//    println("\n\n\n")
//
//    println(lastWindSpeed)
//    println("\n\n\n")
//    // Show DataFrame
////    df.writeStream
////      .outputMode("append")
////      .format("console")
////      .start()
////      .awaitTermination()
////    query.awaitTermination()
//
//
//    if (lastWindSpeed > 4.0) {
//      println("!!!!!!!!!!!!!!!!AAAALLLLEEERRRRTTTT!!!!!!!!!!!!!!!!!!!")
//      // Send an email alert
////      sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", s"Last message wind: $lastMessageWind")
//    }

//    query.awaitTermination()
//
//    // Code after query termination
//    val lastWindSpeed = df.select("wind_mph")
//      .orderBy(desc("timestamp"))
//      .limit(1)
//      .firstOption()
//      .map(_.getDouble(0))
//
//    lastWindSpeed.foreach { windSpeed =>
//      println("\n\n\n")
//      println(windSpeed)
//      println("\n\n\n")
//
//      if (windSpeed > 4.0) {
//        println("!!!!!!!!!!!!!!!!AAAALLLLEEERRRRTTTT!!!!!!!!!!!!!!!!!!!")
//        // Perform actions for high wind alert here
//        // sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", s"Last message wind: $windSpeed")
//      }
//    }


  }

  // Function to send email alert
  def sendEmailAlert(recipient: String, subject: String, body: String): Unit = {
   val properties = new java.util.Properties()
        properties.put("mail.smtp.host", "smtp.gmail.com") // Replace with your SMTP host
        properties.put("mail.smtp.port", "587") // Replace with your SMTP port
        properties.put("mail.smtp.auth", "true")
        properties.put("mail.smtp.starttls.enable", "true")

        val session = Session.getInstance(properties, new javax.mail.Authenticator() {
          override protected def getPasswordAuthentication(): PasswordAuthentication = {
            new PasswordAuthentication("15mscit026@gmail.com", "zvqm ctzt izma xkaa") // Replace with your email and password
          }
        })

        try {
          val message = new MimeMessage(session)
          message.setFrom(new InternetAddress("15mscit026@gmail.com")) // Replace with sender email

          // Replace with recipient email
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