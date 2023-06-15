package citibikesKafkaStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import citibikesKafkaStreaming.streamFunctions._
import citibikesKafkaStreaming.urlToJsonFunctions._
import citibikesKafkaStreaming.jsonSchema._
import com.google.cloud._

object citibikesStream {
  // Set log level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create or get SparkSession
    val spark = createOrGetSparkSession("Bikes Kafka Streaming", "local[3]")

    // Seamless Read from the Kafka Stream
    val kafkaInput = createKafkaReadStream(spark, "localhost", "9092", "citybikeskafka")

    // Converting the serialized Bytes to string
    val kafkaValue = kafkaInput.select("value").withColumn("value", expr("cast(value as string)"))

    // Extract data from JSON and select required fields
    val kafkaDf = kafkaValue.select(from_json(col("value"), kafkaInSchema).alias("bikeDetails"))
      .select("bikeDetails.*")

    val bikesFeedDf = kafkaDf.selectExpr("data.*")
      .selectExpr(
        "en.feeds as en_feeds",
        "es.feeds as es_feeds",
        "fr.feeds as fr_feeds"
      )


    val reqFieldsJson = Seq("system_information", "station_information", "station_status")

    // Process data for "en" language
    val feedEn = feedsFunc(bikesFeedDf, "en", reqFieldsJson)
    val feedEnSystemInfo = feedEn.systemInfoDf
    val feedEnStationInfo = feedEn.stationInfoDf
    val feedEnStationStatus = feedEn.stationStatusDf
    val systemInfoStreamWriterEn = file_writeStream(feedEnSystemInfo,
      "gs://citibike_real_time_en//systemInfo", "checkpoint/check1")
    systemInfoStreamWriterEn.start()

    val stationInfoStreamWriterEn = file_writeStream(feedEnStationInfo,
      "gs://citibike_real_time_en//stationInfo", "checkpoint/check2")
    stationInfoStreamWriterEn.start()

    val stationStatusStreamWriterEn = file_writeStream(feedEnStationStatus,
      "gs://citibike_real_time_en//stationStatus", "checkpoint/check3")
    stationStatusStreamWriterEn.start()

    // Process data for "es" language
    val feedEs = feedsFunc(bikesFeedDf, "es", reqFieldsJson)
    val feedEsSystemInfo = feedEs.systemInfoDf
    val feedEsStationInfo = feedEs.stationInfoDf
    val feedEsStationStatus = feedEs.stationStatusDf
    val systemInfoStreamWriterEs = file_writeStream(feedEsSystemInfo,
      "gs://citibike_real_time_es//systemInfo", "checkpoint/check4")
    systemInfoStreamWriterEs.start()

    val stationInfoStreamWriterEs = file_writeStream(feedEsStationInfo,
      "gs://citibike_real_time_es//stationInfo", "checkpoint/check5")
    stationInfoStreamWriterEs.start()

    val stationStatusStreamWriterEs = file_writeStream(feedEsStationStatus,
      "gs://citibike_real_time_es//stationStatus", "checkpoint/check6")
    stationStatusStreamWriterEs.start()

    // Process data for "fr" language
    val feedFr = feedsFunc(bikesFeedDf, "fr", reqFieldsJson)
    val feedFrSystemInfo = feedFr.systemInfoDf
    val feedFrStationInfo = feedFr.stationInfoDf
    val feedFrStationStatus = feedFr.stationStatusDf
    val systemInfoStreamWriterFr = file_writeStream(feedFrSystemInfo,
      "gs://citibike_real_time_fr//systemInfo", "checkpoint/check7")
    systemInfoStreamWriterFr.start()

    val stationInfoStreamWriterFr = file_writeStream(feedFrStationInfo,
      "gs://citibike_real_time_fr//stationInfo", "checkpoint/check8")
    stationInfoStreamWriterFr.start()

    val stationStatusStreamWriterFr = file_writeStream(feedFrStationStatus,
      "gs://citibike_real_time_fr//stationStatus", "checkpoint/check9")
    stationStatusStreamWriterFr.start()


    spark.streams.awaitAnyTermination()
  }

}
