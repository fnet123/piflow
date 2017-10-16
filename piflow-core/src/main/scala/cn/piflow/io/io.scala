package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

//////////////source//////////////

trait Source {
	def init(ctx: RunnerContext): Unit = {}

	def destroy(): Unit = {}
}

trait BatchSource extends Source {
	def loadBatch(): Dataset[_];
}

trait StreamSource extends Source {
	def getOffset: Long;

	def schema(): StructType;

	def loadBatch(start: Long, end: Long): Dataset[_];
}

////////////// sink //////////////

trait Sink {
	def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {}

	def destroy(): Unit = {}
}

trait BatchSink extends Sink {
	def writeBatch(ds: Dataset[_]): Unit;
}

trait StreamSink extends Sink {
	def writeBatch(batchId: Long, data: Dataset[_]): Unit;
}