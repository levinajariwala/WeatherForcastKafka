package KafkaFunctionality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import javax.mail._
import javax.mail.internet._

object ReadFromKafkaHBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToHBase").master("local[*]").getOrCreate()

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
      StructField("localtime", StringType, nullable = true)
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
      .withColumn("is_alert", when(col("wind_mph") > 8.0, 1).otherwise(0))

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val lastRecordDF = batchDF.orderBy(col("localtime").desc).limit(1)


          if (!lastRecordDF.isEmpty) {
            val windSpeed = lastRecordDF.select("wind_mph").first().getAs[Double]("wind_mph")

            if (windSpeed > 8.0) {
              sendEmailAlert("levinajariwala@gmail.com", "High Wind Alert", "High wind speed detected!")
              println("High wind speed detected!")
            }
          }

          writeToHBase(batchDF)
        }
      }
      .start()

    query.awaitTermination()
  }

  def writeToHBase(batchDF: DataFrame): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "your_hbase_zookeeper_quorum")

    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf("your_hbase_table_name"))

    try {
      batchDF.foreachPartition { partition =>
        partition.foreach { row =>
          val put = new Put(Bytes.toBytes(row.getAs[String]("localtime")))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("wind_mph"), Bytes.toBytes(row.getAs[Double]("wind_mph").toString))
          // Add other columns as required using put.addColumn()

          table.put(put)
        }
      }
    } finally {
      table.close()
      connection.close()
    }
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
