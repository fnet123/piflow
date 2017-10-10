package cn.piflow.processor.io

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.RunnerContext
import cn.piflow.io._
import cn.piflow.processor.{Processor021, Processor120}
import cn.piflow.util.ReflectUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{Sink => SparkStreamSink, Source => SparkStreamSource, _}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types.StructType

/**
	* @author bluejoe2008@gmail.com
	*/
case class DoLoad(source: Source, outputMode: OutputMode = OutputMode.Complete)
	extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		source match {
			case bs: BatchSource => loadBatch(bs, ctx);
			case ss: StreamSource => loadStream(ss, ctx);
		}
	}

	def loadBatch(bs: BatchSource, ctx: RunnerContext): Dataset[_] = {
		bs.init(ctx);
		bs.loadDataset();
	}

	def loadStream(ss: StreamSource, ctx: RunnerContext): Dataset[_] = {
		//TODO: hard code here
		ctx.forType[SparkSession].readStream.load();
	}

	def asSparkStreamSource(ss: StreamSource, ctx: RunnerContext) = new SparkStreamSource() {
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
	override def perform(input: Any, ctx: RunnerContext) {
		val ds: Dataset[_] = input.asInstanceOf[Dataset[_]];
		if (ds.isStreaming)
			performStream(sink.asInstanceOf[StreamSink], ds, ctx);
		else
			performBatch(sink.asInstanceOf[BatchSink], ds, ctx);
	}

	def performStream(ss: StreamSink, ds: Dataset[_], ctx: RunnerContext) {
		val df = ds.toDF();
		ss.init(outputMode, ctx);
		val query = df.sparkSession._get("sessionState").
			_get("streamingQueryManager").
			_call("startQuery")(
				DoWrite.getNextQueryId,
				ctx("checkpointLocation").asInstanceOf[String],
				df,
				asSparkStreamSink(ss, ctx),
				outputMode,
				ss.useTempCheckpointLocation,
				ss.recoverFromCheckpointLocation,
				ProcessingTime(0) //start now
			).asInstanceOf[StreamingQuery];

		query.awaitTermination();
	}

	def asSparkStreamSink(ss: StreamSink, ctx: RunnerContext) = new SparkStreamSink() {
		def addBatch(batchId: Long, data: DataFrame): Unit = {
			ss.addBatch(batchId, data);
		}
	}

	def performBatch(bs: BatchSink, ds: Dataset[_], ctx: RunnerContext): Unit = {
		bs.init(outputMode, ctx);
		bs.saveBatch(ds);
	}
}

object DoWrite {
	val queryId = new AtomicInteger(0);

	def getNextQueryId() = "query-" + queryId.incrementAndGet();
}
