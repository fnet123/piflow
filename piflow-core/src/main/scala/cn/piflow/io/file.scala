package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink, ConsoleSink}
import org.apache.spark.sql.{SparkSession, Dataset, DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.execution.datasources.{FileFormat => SparkFileFormat}
import org.apache.spark.sql.execution.streaming.{FileStreamSink => SparkFileStreamSink}

/**
	* Created by bluejoe on 2017/10/10.
	*/
case class FileSink(fileFormat: String, path: String) extends SparkSinkAdapter {

	override def build(writer: DataFrameWriter[_]): DataFrameWriter[_] = {
		writer.format(fileFormat).option("path", path);
	}
}

case class FileSource(fileFormat: String, path: String) extends SparkSourceAdapter {

	override def build(reader: DataFrameReader): DataFrameReader = {
		reader.format(fileFormat).option("path", path);
	}
}

object FileFormat {
	lazy val TEXT = new FileFormat() {
		def create() = new TextFileFormat();
	}
	lazy val PARQUET = new FileFormat() {
		def create() = new ParquetFileFormat();
	}
	lazy val JSON = new FileFormat() {
		def create() = new JsonFileFormat();
	}
}

trait FileFormat {
	def create(): SparkFileFormat;
}

case class FileStreamSink(path: String, fileFormat: FileFormat, partitionColumnNames: Seq[String] = Seq[String]())
	extends SparkStreamSinkAdapter with StreamSink {
	override def createSparkStreamSink(outputMode: OutputMode, ctx: JobContext) =
		new SparkFileStreamSink(ctx.sparkSession(),
			path,
			fileFormat.create(),
			partitionColumnNames,
			Map[String, String]());

	override def toString = this.getClass.getSimpleName;
}