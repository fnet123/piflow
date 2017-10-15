package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

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