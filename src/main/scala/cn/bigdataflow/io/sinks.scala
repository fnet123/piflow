package cn.bigdataflow.io

import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 */
case class ConsoleSink[T]() extends BatchSink[T] {
	override def toString = this.getClass.getSimpleName;
	def consumeDataset(ds: Dataset[T], ctx: RunnerContext) = {
		ds.show();
	}
}

case class MemorySink[T: ClassTag]() extends BatchSink[T] {
	override def toString = this.getClass.getSimpleName;
	val buffer = ArrayBuffer[T]();

	def consumeDataset(ds: Dataset[T], ctx: RunnerContext) = {
		buffer ++= ds.collect();
	}

	def get(): Seq[T] = buffer.toSeq
}