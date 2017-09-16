package cn.bigdataflow.lib.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import cn.bigdataflow.RunnerContext
import scala.reflect.ManifestFactory.classType
import org.apache.spark.sql.streaming.StreamingQuery
import cn.bigdataflow.BatchSink

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