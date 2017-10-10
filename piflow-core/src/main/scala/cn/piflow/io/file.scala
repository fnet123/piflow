package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.streaming.OutputMode

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
