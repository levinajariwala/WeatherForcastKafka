import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()

    val dbProperties = new Properties()
    dbProperties.setProperty("driver", "org.postgresql.Driver")
    dbProperties.setProperty("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
    dbProperties.setProperty("user", "consultants")
    dbProperties.setProperty("password", "WelcomeItc@2022")
    // Specify the table you want to read
    val tableName = "b1"

    // Read data from PostgreSQL into a DataFrame
    val df = spark.read.jdbc(dbProperties.getProperty("url"), tableName, dbProperties)

    // Show the DataFrame
    df.show()
    // Define your database
    val databaseName = "bduk"

    data.write.mode(SaveMode.Overwrite).option("path", s"/warehouse/tablespace/external/hive/$databaseName.db/b11").saveAsTable(s"$databaseName.b11")

  }
}