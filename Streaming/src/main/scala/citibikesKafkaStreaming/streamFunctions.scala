package citibikesKafkaStreaming

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

object streamFunctions {

  def createOrGetSparkSession(app_name: String, master: String = "yarn") = {
    """
      Creates or gets a Spark Session
      Parameters:
          app_name : str
              Pass the name of your app
          master : str
              Choosing the Spark master, yarn is the default
      Returns:
          spark: SparkSession
      """

    SparkSession
      .builder
      .appName(app_name)
      .master(master = master)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
  }


  def createKafkaReadStream(spark: SparkSession,
                            kafka_address: String,
                            kafka_port: String,
                            topic: String,
                            starting_offset: String = "latest"
                           ): DataFrame = {
    """
      Creates a kafka read stream

      Parameters:
          spark : SparkSession
              A SparkSession object
          kafka_address: str
              Host address of the kafka bootstrap server
          topic : str
              Name of the kafka topic
      Returns:
          read_stream: DataFrame
      """

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafka_address:$kafka_port")
      .option("subscribe", topic)
      .option("startingOffsets", starting_offset)
      .load()
  }

  def file_writeStream(dataFrameStream: DataFrame,
                       storage_path: String,
                       checkpoint_path: String,
                       output_mode: String = "append",
                       triggerTime: String = "180 seconds",
                       file_format: String = "parquet"): DataStreamWriter[Row] = {

    """
        Write the stream back to a storage location

        Parameters:
            stream : DataStreamReader
                The data stream reader for your stream
            file_format : str
                parquet, csv, orc etc
            storage_path : str
                The file output path
            checkpoint_path : str
                The checkpoint location for spark
            trigger : str
                The trigger interval
            output_mode : str
                append, complete, update
        """

    dataFrameStream.writeStream
      .format(file_format)
      .outputMode(output_mode)
      .option("path", storage_path)
      .option("checkpointLocation", checkpoint_path)
      .partitionBy("year", "month", "day", "hour")
      .trigger(Trigger.ProcessingTime(triggerTime))
  }

}
