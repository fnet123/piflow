package cn.piflow.io

import org.apache.spark.sql.SparkSession
import org.junit.Test

//this test class helps debugging
class SparkTest {
	val cronExpr = "*/5 * * * * ";
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp");

	import spark.implicits._

	@Test
	def testIO() = {
		val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
		peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

		val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
		usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

		val jdbcDF = spark.read
			.format("jdbc")
			.option("url", "jdbc:postgresql:dbserver")
			.option("dbtable", "schema.tablename")
			.option("user", "username")
			.option("password", "password")
			.load()

		jdbcDF.write
			.format("jdbc")
			.option("url", "jdbc:postgresql:dbserver")
			.option("dbtable", "schema.tablename")
			.option("user", "username")
			.option("password", "password")
			.save()
	}

	@Test
	def testStreamIO(): Unit = {
		val lines = spark.readStream
			.format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()

		// Split the lines into words
		val words = lines.as[String].flatMap(_.split(" "))

		// Generate running word count
		val wordCounts = words.groupBy("value").count()
		val query = wordCounts.writeStream
			.outputMode("complete")
			.format("console")
			.start()

		query.awaitTermination()
	}
}