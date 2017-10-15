package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode}
import org.apache.spark.sql.execution.streaming.{Sink => SparkStreamSink, Source => SparkStreamSource, _}
import org.apache.spark.sql.types.StructType

/**
	* Created by bluejoe on 2017/10/10.
	*/
//TODO: spark code is too dirty, Sink & Source interfaces should be commonly used, adapters are not recommended
abstract class SparkSinkAdapter extends BatchSink {
	def build(writer: DataFrameWriter[_]): DataFrameWriter[_];

	var _outputMode: OutputMode = null;

	override def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {
		_outputMode = outputMode;
	}

	def saveBatch(ds: Dataset[_]): Unit = {
		val writer = ds.write.mode(SparkIOSupport.outputMode2SaveMode(_outputMode));
		build(writer).save();
	}

	override def destroy(): Unit = {

	}
}

abstract class SparkSourceAdapter extends BatchSource {
	def build(reader: DataFrameReader): DataFrameReader;

	var _spark: SparkSession = null;

	override def init(ctx: RunnerContext): Unit = {
		_spark = ctx.forType[SparkSession];
	}

	def loadDataset(): Dataset[_] = {
		val reader = _spark.read;
		build(reader).load();
	}

	override def destroy(): Unit = {

	}
}

abstract class SparkStreamSinkAdapter extends StreamSink {
	def createSparkStreamSink(outputMode: OutputMode, ctx: RunnerContext): SparkStreamSink;
	var _sparkStreamSink: SparkStreamSink = null;

	override def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {
		_sparkStreamSink = createSparkStreamSink(outputMode, ctx);
	}

	override def addBatch(batchId: Long, data: DataFrame) {
		_sparkStreamSink.addBatch(batchId, data);
	}

	override def destroy(): Unit = {

	}
}

abstract class SparkStreamSourceAdapter extends StreamSource {
	def createSparkStreamSource(ctx: RunnerContext): SparkStreamSource;
	var _sparkStreamSource: SparkStreamSource = null;

	override def schema(): StructType = _sparkStreamSource.schema;

	override def init(ctx: RunnerContext) = {
		_sparkStreamSource = createSparkStreamSource(ctx);
	}

	private def discard(start: Long, end: Long) = {
		//TODO
		if (false)
			_sparkStreamSource.commit(SparkIOSupport.toOffset(end));
	}

	override def destroy(): Unit = {
		_sparkStreamSource.stop();
	}

	def getOffset: Long = {
		SparkIOSupport.valueOf(_sparkStreamSource.getOffset);
	}

	def loadBatch(start: Long, end: Long): Dataset[_] = {
		val ds = _sparkStreamSource.getBatch(SparkIOSupport.toOffsetOption(start), SparkIOSupport.toOffsetOption(end).get);
		discard(start, end);
		ds;
	}
}

