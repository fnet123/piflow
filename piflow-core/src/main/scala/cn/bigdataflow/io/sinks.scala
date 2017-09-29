package cn.bigdataflow.io

import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 */
case class ConsoleSink() extends BatchSink {
	override def toString = this.getClass.getSimpleName;
	def consumeDataset(ds: Dataset[_], ctx: RunnerContext) = {
		ds.show();
	}
}

case class MemorySink() extends BatchSink {
	override def toString = this.getClass.getSimpleName;
	val buffer = ArrayBuffer[Any]();

	def consumeDataset(ds: Dataset[_], ctx: RunnerContext) = {
		buffer ++= ds.collect();
	}

	def get(): Seq[_] = buffer.toSeq
}