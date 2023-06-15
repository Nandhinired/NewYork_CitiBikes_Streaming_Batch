package citibikesKafkaStreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import citibikesKafkaStreaming.jsonSchema._
import scala.io.Source

object urlToJsonFunctions {

  case class feed(systemInfoDf: DataFrame, stationInfoDf: DataFrame, stationStatusDf: DataFrame)

  // Defining a UDF to fetch data from URL
  val fetchDataUDF = udf((url: String) => Source.fromURL(url).mkString)


  def feedsFunc(inputFeedDf: DataFrame,
                value: String,
                reqFields: Seq[String]) = {

    // Select the desired language feed and explode the feeds array
    val valueDf = inputFeedDf.select(col(value + "_feeds"))
      .withColumn(value + "_feeds", explode(col(value + "_feeds")))
      .selectExpr(value + "_feeds.*")

    // Filter the rows based on required fields
    val valueFdf = valueDf.filter(col("name").isin(reqFields: _*))

    val valueUrlDf = valueFdf.withColumn("url", fetchDataUDF(col("url")))

    // system_information
    val valueSystemInfoDf = systemInfoFunc(valueUrlDf)

    //   station_information
    val valueStationInfoDf = stationInfoFunc(valueUrlDf)

    //    station_status
    val valueStationStatusDf = stationStatusFunc(valueUrlDf)

    //Returns feed(dataframe,Dataframe,Dataframe)
    feed(valueSystemInfoDf, valueStationInfoDf, valueStationStatusDf)

  }


  def systemInfoFunc(valueDf: DataFrame): DataFrame = {

    // Filter rows for system_information
    val systemInfoDf = valueDf.filter(col("name") === "system_information")
      .select("url")

    // Parse JSON and select required fields
    val systemInfoJsonDf = systemInfoDf.withColumn("url", from_json(col("url"), systemInfoSchema))
    val systemInfoJsonTab = systemInfoJsonDf.selectExpr("url.*")

    // Select additional columns and perform data transformations
    systemInfoJsonTab.selectExpr("data.*", "last_updated", "ttl")
      .withColumn("last_updated", from_unixtime(col("last_updated")))
      .withColumn("year", year(col("last_updated")))
      .withColumn("month", month(col("last_updated")))
      .withColumn("hour", hour(col("last_updated")))
      .withColumn("day", dayofmonth(col("last_updated")))
  }


  def stationInfoFunc(valueDf: DataFrame): DataFrame = {

    // Filter rows for station_information
    val stationInfoDf = valueDf.filter(col("name") === "station_information")
      .select("url")

    // Parse JSON and select required fields
    val stationInfoJsonDf = stationInfoDf.withColumn("url", from_json(col("url"), stationInfoSchema))
    val stationInfoJsonTab = stationInfoJsonDf.selectExpr("url.*")

    // Explode data.stations, select additional columns, and perform data transformations
    stationInfoJsonTab.selectExpr("explode(data.stations)", "last_updated", "ttl")
      .selectExpr("col.*", "last_updated", "ttl")
      .withColumn("last_updated", from_unixtime(col("last_updated")))
      .withColumn("rental_methods", explode(col("rental_methods")))
      .withColumnRenamed("name", "station_name")
      .withColumn("eightd_station_services", explode(col("eightd_station_services")))
      .withColumn("eightd_station_services", from_json(col("eightd_station_services"), eightdStationSchema))
      .selectExpr("*", "rental_uris.*", "eightd_station_services.*")
      .drop("rental_uris", "eightd_station_services")
      .withColumn("year", year(col("last_updated")))
      .withColumn("month", month(col("last_updated")))
      .withColumn("hour", hour(col("last_updated")))
      .withColumn("day", dayofmonth(col("last_updated")))
  }


  def stationStatusFunc(valueDf: DataFrame): DataFrame = {

    // Filter rows for station_status
    val stationStatusDf = valueDf.filter(col("name") === "station_status")
      .select("url")

    // Parse JSON and select required fields
    val stationStatusJsonDf = stationStatusDf.withColumn("url", from_json(col("url"), stationStatusSchema))
    val stationStatusJsonTab = stationStatusJsonDf.selectExpr("url.*")

    // Explode data.stations, select additional columns, and perform data transformations
    stationStatusJsonTab.selectExpr("explode(data.stations)", "last_updated", "ttl")
      .selectExpr("col.*", "last_updated", "ttl")
      .withColumn("last_reported", from_unixtime(col("last_reported")))
      .withColumn("year", year(col("last_reported")))
      .withColumn("month", month(col("last_reported")))
      .withColumn("hour", hour(col("last_reported")))
      .withColumn("day", dayofmonth(col("last_reported")))
  }


}
