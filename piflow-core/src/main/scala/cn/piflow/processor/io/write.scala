package cn.piflow.processor.io

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.JobContext
import cn.piflow.io._
import cn.piflow.processor.{Processor021, Processor120}
import cn.piflow.runner.ProcessedRows
import cn.piflow.util.ReflectUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{Sink => SparkStreamSink, Source => SparkStreamSource, _}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SystemClock

/**
	* @author bluejoe2008@gmail.com
	*/
case class DoWrite(sink: Sink, outputMode: OutputMode = OutputMode.Complete)
	extends Processor120 {
	override def perform120(input: Any) {
		val ds: Dataset[_] = input.asInstanceOf[Dataset[_]];
		//stream comes from a StreamSource
		if (ds.isStreaming)
			saveStream(sink.asInstanceOf[StreamSink], ds);
		else
			saveBatch(sink.asInstanceOf[BatchSink], ds);
	}

	//TODO: bad code here
	def saveStream(ss: StreamSink, ds: Dataset[_]) {
		val df = ds.toDF();
		ss.init(outputMode, _processorContext);

		val sqm = df.sparkSession._get("sessionState").
			_getLazy("streamingQueryManager");

		val query = sqm._call("startQuery")(
			None, //Utils.getNextQueryId,
			None, //ctx("checkpointLocation").asInstanceOf[String],
			df,
			asSparkStreamSink(ss),
			outputMode,
			false, //ss.useTempCheckpointLocation,
			true, //ss.recoverFromCheckpointLocation,
			ProcessingTime(0), //start now
			instanceOf("org.apache.spark.util.SystemClock")()
		);

		query.asInstanceOf[StreamingQuery].awaitTermination();
	}

	private def asSparkStreamSink(ss: StreamSink) = new SparkStreamSink() {
		def addBatch(batchId: Long, data: DataFrame): Unit = {
			ss.writeBatch(batchId, data);
			//TODO: too expensive count()
			notifyEvent(ProcessedRows(Some(data.count()), None));
		}
	}

	def saveBatch(bs: BatchSink, ds: Dataset[_]): Unit = {
		bs.init(outputMode, _processorContext);
		bs.writeBatch(ds);

		//TODO: too expensive count()
		notifyEvent(ProcessedRows(Some(ds.count()), None));
	}
}