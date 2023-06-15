package citibikesKafkaStreaming

import org.apache.spark.sql.types._

object jsonSchema {

  // Schema for the input data from Kafka
  val kafkaInSchema = StructType(List(
    StructField("data", StructType(List(
      StructField("en", StructType(List(
        StructField("feeds", ArrayType(StructType(List(
          StructField("name", StringType),
          StructField("url", StringType)))))))),
      StructField("es", StructType(List(
        StructField("feeds", ArrayType(StructType(List(
          StructField("name", StringType),
          StructField("url", StringType)))))))),
      StructField("fr", StructType(List(
        StructField("feeds", ArrayType(StructType(List(
          StructField("name", StringType),
          StructField("url", StringType))))))))))),
    StructField("last_updated", LongType),
    StructField("ttl", LongType)))

  // Schema for the system_information json data
  val systemInfoSchema = StructType(Array(
    StructField("data", StructType(Array(
      StructField("language", StringType),
      StructField("name", StringType),
      StructField("operator", StringType),
      StructField("phone_number", StringType),
      StructField("short_name", StringType),
      StructField("system_id", StringType),
      StructField("timezone", StringType),
      StructField("url", StringType)))),
    StructField("last_updated", LongType),
    StructField("ttl", LongType)))

  // Schema for the station_information json data
  val stationInfoSchema = StructType(List(
    StructField("data", StructType(List(
      StructField("stations", ArrayType(StructType(List(
        StructField("address", StringType),
        StructField("capacity", LongType),
        StructField("client_station_id", StringType),
        StructField("dockless_bikes_parking_zone_capacity", LongType),
        StructField("eightd_has_key_dispenser", BooleanType),
        StructField("eightd_station_services", ArrayType(StringType)),
        StructField("electric_bike_surcharge_waiver", BooleanType),
        StructField("external_id", StringType),
        StructField("has_kiosk", BooleanType),
        StructField("lat", DoubleType),
        StructField("legacy_id", StringType),
        StructField("lon", DoubleType),
        StructField("name", StringType),
        StructField("rack_model", StringType),
        StructField("region_code", StringType),
        StructField("region_id", StringType),
        StructField("rental_methods", ArrayType(StringType)),
        StructField("rental_uris", StructType(List(
          StructField("android", StringType),
          StructField("ios", StringType)))),
        StructField("short_name", StringType),
        StructField("station_id", StringType),
        StructField("station_type", StringType),
        StructField("target_bike_capacity", LongType),
        StructField("target_scooter_capacity", LongType)))))))),
    StructField("last_updated", LongType),
    StructField("ttl", LongType)
  ))

  // Schema for the eightd_station_services json data
  val eightdStationSchema = StructType(List(
    StructField("bikes_availability", StringType),
    StructField("description", StringType),
    StructField("docks_availability", StringType),
    StructField("id", StringType),
    StructField("name", StringType),
    StructField("off_dock_bikes_count", LongType),
    StructField("off_dock_remaining_bike_capacity", LongType),
    StructField("service_type", StringType)))


  val stationStatusSchema = StructType(Array(
    StructField("data", StructType(Array(
      StructField("stations", ArrayType(
        StructType(Array(
          StructField("eightd_has_available_keys", BooleanType),
          StructField("is_installed", LongType),
          StructField("is_renting", LongType),
          StructField("is_returning", LongType),
          StructField("last_reported", LongType),
          StructField("legacy_id", StringType),
          StructField("num_bike_spots_available", LongType),
          StructField("num_bikes_available", LongType),
          StructField("num_bikes_disabled", LongType),
          StructField("num_docks_available", LongType),
          StructField("num_docks_disabled", LongType),
          StructField("num_ebikes_available", LongType),
          StructField("num_scooter_spots_available", LongType),
          StructField("num_scooters_available", LongType),
          StructField("num_scooters_unavailable", LongType),
          StructField("station_id", StringType),
          StructField("station_status", StringType)))))))),
    StructField("last_updated", LongType),
    StructField("ttl", LongType)
  ))


}
