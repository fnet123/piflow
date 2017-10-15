package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}
import org.apache.spark.sql.streaming.OutputMode

trait Sink {
	def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {}

	def destroy(): Unit = {}
}

trait BatchSink extends Sink {
	def saveBatch(ds: Dataset[_]): Unit;
}

trait StreamSink extends Sink {
	def addBatch(batchId: Long, data: DataFrame): Unit;

	def useTempCheckpointLocation: Boolean = false;

	def recoverFromCheckpointLocation: Boolean = true;
}