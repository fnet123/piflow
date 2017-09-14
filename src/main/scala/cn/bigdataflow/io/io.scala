package cn.bigdataflow.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import cn.bigdataflow.RunnerContext
import scala.reflect.ManifestFactory.classType

/**
 * @author bluejoe2008@gmail.com
 */
trait BucketSource[X] {
	def createDataset(ctx: RunnerContext): Dataset[X];
}

trait BucketSink[X] {
	def consumeDataset(ds: Dataset[X], ctx: RunnerContext);
}

case class ConsoleSink[T]() extends BucketSink[T] {
	override def toString = this.getClass.getSimpleName;
	def consumeDataset(ds: Dataset[T], ctx: RunnerContext) = {
		ds.show();
	}
}

case class SeqAsSource[X: Encoder](t: X*) extends BucketSource[X] {
	override def toString = this.getClass.getSimpleName;
	def createDataset(ctx: RunnerContext): Dataset[X] = {
		val spark = ctx.forType[SparkSession]();
		spark.createDataset(t);
	}
}

case class MemorySink[T: ClassTag]() extends BucketSink[T] {
	override def toString = this.getClass.getSimpleName;
	val buffer = ArrayBuffer[T]();

	def consumeDataset(ds: Dataset[T], ctx: RunnerContext) = {
		buffer ++= ds.collect();
	}

	def get(): Seq[T] = buffer.toSeq
}
