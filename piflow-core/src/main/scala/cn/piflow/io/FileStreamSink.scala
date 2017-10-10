package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink => SparkFileStreamSink}
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/9.
	*/
class FileStreamSink(path: String, fileFormat: FileFormat, partitionColumnNames: Seq[String], options: Map[String, String])
	extends StreamSink {
	var _sparkFileStreamSink: SparkFileStreamSink = null;

	override def destroy(): Unit = {

	}

	override def addBatch(batchId: Long, data: DataFrame): Unit =
		_sparkFileStreamSink.addBatch(batchId, data);

	override def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {
		_sparkFileStreamSink = new SparkFileStreamSink(ctx.forType[SparkSession],
			path: String,
			fileFormat: FileFormat,
			partitionColumnNames: Seq[String],
			options: Map[String, String]);
	}

	override def useTempCheckpointLocation(): Boolean = false;

	override def recoverFromCheckpointLocation() = true;
}
