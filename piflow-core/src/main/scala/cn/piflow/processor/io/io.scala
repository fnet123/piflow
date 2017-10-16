package cn.piflow.processor.io

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.RunnerContext
import cn.piflow.io._
import cn.piflow.processor.{Processor021, Processor120}
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
case class DoLoad(source: Source)
	extends Processor021 {
	override def perform021(ctx: RunnerContext): Dataset[_] = {
		source match {
			case bs: BatchSource => loadBatch(bs, ctx);
			case ss: StreamSource => loadStream(ss, ctx);
		}
	}

	def loadBatch(bs: BatchSource, ctx: RunnerContext): Dataset[_] = {
		bs.init(ctx);
		bs.loadBatch();
	}

	//TODO: bad code here
	def loadStream(ss: StreamSource, ctx: RunnerContext): Dataset[_] = {
		ss.init(ctx);
		// Dataset.ofRows(sparkSession, StreamingRelation(dataSource))
		val sparkSession = ctx.forType[SparkSession];

		class SparkDataSource extends DataSource(sparkSession, "NeverUsedProviderClassName") {
			override def createSource(metadataPath: String) = asSparkStreamSource(ss, ctx);
			override lazy val sourceInfo = SourceInfo(Utils.getNextStreamSourceName(), ss.schema, Nil)
		}

		val dataSource = new SparkDataSource();
		val sr = StreamingRelation(dataSource);
		singleton[Dataset[Row]]._call("ofRows")(sparkSession, sr).asInstanceOf[Dataset[_]];
	}

	private def asSparkStreamSource(ss: StreamSource, ctx: RunnerContext) = new SparkStreamSource() {
		override def schema: StructType = null;


		override def getOffset: Option[Offset] = {
			SparkIOSupport.toOffsetOption(ss.getOffset);
		}

		override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
			ss.loadBatch(SparkIOSupport.valueOf(start), SparkIOSupport.valueOf(end)).toDF();
		}

		override def commit(end: Offset): Unit = {}

		override def stop(): Unit = {}
	}
}

case class DoWrite(sink: Sink, outputMode: OutputMode = OutputMode.Complete)
	extends Processor120 {
	override def perform120(input: Any, ctx: RunnerContext) {
		val ds: Dataset[_] = input.asInstanceOf[Dataset[_]];
		//stream comes from a StreamSource
		if (ds.isStreaming)
			saveStream(sink.asInstanceOf[StreamSink], ds, ctx);
		else
			saveBatch(sink.asInstanceOf[BatchSink], ds, ctx);
	}

	//TODO: bad code here
	def saveStream(ss: StreamSink, ds: Dataset[_], ctx: RunnerContext) {
		val df = ds.toDF();
		ss.init(outputMode, ctx);

		val sqm = df.sparkSession._get("sessionState").
			_getLazy("streamingQueryManager");

		val query = sqm._call("startQuery")(
			None, //Utils.getNextQueryId,
			None, //ctx("checkpointLocation").asInstanceOf[String],
			df,
			asSparkStreamSink(ss, ctx),
			outputMode,
			false, //ss.useTempCheckpointLocation,
			true, //ss.recoverFromCheckpointLocation,
			ProcessingTime(0), //start now
			instanceOf("org.apache.spark.util.SystemClock")()
		);

		query.asInstanceOf[StreamingQuery].awaitTermination();
	}

	private def asSparkStreamSink(ss: StreamSink, ctx: RunnerContext) = new SparkStreamSink() {
		def addBatch(batchId: Long, data: DataFrame): Unit = {
			ss.writeBatch(batchId, data);
		}
	}

	def saveBatch(bs: BatchSink, ds: Dataset[_], ctx: RunnerContext): Unit = {
		bs.init(outputMode, ctx);
		bs.writeBatch(ds);
	}
}

private object Utils {
	val queryId = new AtomicInteger(0);
	val sourceId = new AtomicInteger(0);

	def getNextStreamSourceName() = "stream-source-" + sourceId.incrementAndGet();

	//TODO: actually never used
	def getNextQueryId() = "query-" + queryId.incrementAndGet();
}
