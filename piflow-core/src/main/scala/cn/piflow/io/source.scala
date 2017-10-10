package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._

trait Source {
	def init(ctx: RunnerContext);

	def destroy();
}

trait BatchSource extends Source {
	def loadDataset(): Dataset[_];
}

trait StreamSource extends Source {
	def getOffset: Long;

	def loadBatch(start: Long, end: Long): Dataset[_];
}