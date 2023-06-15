package citibikesKafkaStreaming

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import citibikesKafkaStreaming.urlToJsonFunctions._


case class myRow(short_name: String, language: String, system_id: String, phone_number: String, url: String, timezone: String, name: String, operator: String, last_updated: String, ttl: String, year: Int, month: Int, hour: Int, day: Int)

class citibikesTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var systemInfoDf: DataFrame = _

  override def beforeAll(): Unit = {
    // Staring the SparkSession
    spark = SparkSession.builder().
      master("local[2]")
      .getOrCreate()

    // Defining the schema
    val mySchema = StructType(List(
      StructField("name", StringType),
      StructField("url", StringType)
    ))

    // Creating the test Dataframe
    val myRows = List(Row("system_information", """{"data":{"short_name":"Citi Bike","language":"en","system_id":"nyc","phone_number":"855-245-3311","url":"https://www.lyft.com/bikes","timezone":"America/New_York","name":"Citi Bike","operator":"Lyft, Inc"},"last_updated":1686264803,"ttl":5}"""),
      Row("dummy_information", """{"data":{"short_name":"New Bike","language":"er","system_id":"nyk","phone_number":"855-675-3311","url":"https://www.lyft.com/bikes","timezone":"America/New_York","name":"New Bike","operator":"karl, Inc"},"last_updated":16862678803,"ttl":9}"""))
    val myRdd = spark.sparkContext.parallelize(myRows)
    systemInfoDf = spark.createDataFrame(myRdd, mySchema)

  }

  // Clean up resources after all tests are finished
  override def afterAll(): Unit = {
    spark.stop()
  }

  test("test data type") {
    val spark2 = spark
    import spark2.implicits._

    // Call the systemInfoFunc function on the input DataFrame
    val resultDf = systemInfoFunc(systemInfoDf)

    // Collect the actual and expected data as Array[Row]
    val actualData = resultDf.as[myRow].collect()
    val expectedData = Array(
      myRow("Citi Bike", "en", "nyc", "855-245-3311", "https://www.lyft.com/bikes", "America/New_York", "Citi Bike", "Lyft, Inc",
        "2023-06-09 00:53:23", "5", 2023, 6, 0, 9)
    )

    // Assert the transformation result by comparing each column of the collected data with the expected values

    assert(actualData.length == expectedData.length)

    for (i <- actualData.indices) {
      val actual = actualData(i)
      val expected = expectedData(i)

      assert(actual.name == expected.name)
      assert(actual.language == expected.language)
      assert(actual.system_id == expected.system_id)
      assert(actual.phone_number == expected.phone_number)
      assert(actual.url == expected.url)
      assert(actual.timezone == expected.timezone)
      assert(actual.short_name == expected.short_name)
      assert(actual.operator == expected.operator)
      assert(actual.last_updated == expected.last_updated)
      assert(actual.ttl == expected.ttl)
      assert(actual.year == expected.year)
      assert(actual.month == expected.month)
      assert(actual.hour == expected.hour)
      assert(actual.day == expected.day)
    }


  }


}
