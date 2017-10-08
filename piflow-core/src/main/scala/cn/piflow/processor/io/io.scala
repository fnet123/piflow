package cn.piflow.processor.io

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.RunnerContext
import cn.piflow.io.{BatchSink, BatchSource, Sink, StreamSink}
import cn.piflow.processor.{Processor021, Processor120}
import cn.piflow.util.ReflectUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{Sink => SparkStreamSink}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}

/**
	* @author bluejoe2008@gmail.com
	*/
object DoLoad {
	def apply(format: String, args: Map[String, String] = Map()) = {
		new _DoLoadDefinedSource(format, args);
	}

	def apply(source: BatchSource) = {
		new _DoLoadSource(source);
	}
}

case class _DoLoadSource(source: BatchSource) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		source.createDataset(ctx);
	}
}

case class _DoLoadDefinedSource(format: String, args: Map[String, String]) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		ctx.forType[SparkSession].read.format(format).options(args).load();
	}
}

case class DoLoadStream(format: String, args: Map[String, String]) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		val df = ctx.forType[SparkSession].readStream.format(format).options(args).load();
		df;
	}
}

/*
case class DoLoadStream2(source: StreamSource) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		Dataset.ofRows(sparkSession, StreamingRelation(dataSource));
		val df = ctx.forType[SparkSession].readStream.format(format).options(args).load();
		df;
	}
}
*/

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
		val query = df.sparkSession.doGet("sessionState").
			doGet("streamingQueryManager").
			doCall("startQuery")(
				DoWrite.getNextQueryId,
				ctx("checkpointLocation").asInstanceOf[String],
				df,
				asSparkStreamSink(ss, ctx),
				outputMode,
				ss.useTempCheckpointLocation(outputMode, ctx),
				ss.recoverFromCheckpointLocation(outputMode, ctx),
				ProcessingTime(0) //start now
			).asInstanceOf[StreamingQuery];

		query.awaitTermination();
	}

	def asSparkStreamSink(ss: StreamSink, ctx: RunnerContext) = new SparkStreamSink() {
		def addBatch(batchId: Long, data: DataFrame): Unit = {
			ss.addBatch(batchId, data, outputMode, ctx);
		}
	}

	def performBatch(bs: BatchSink, ds: Dataset[_], ctx: RunnerContext): Unit = {
		bs.saveDataset(ds, outputMode, ctx);
	}
}

object DoWrite {
	val queryId = new AtomicInteger(0);

	def getNextQueryId() = "query-" + queryId.incrementAndGet();
}
