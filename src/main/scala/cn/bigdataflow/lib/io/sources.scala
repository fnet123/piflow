package cn.bigdataflow.lib.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import cn.bigdataflow.RunnerContext
import scala.reflect.ManifestFactory.classType
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.execution.streaming.{ Source ⇒ SparkStreamSource };
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.StreamingExecutionRelation
import org.apache.spark.sql.execution.streaming.MemoryStream
import cn.bigdataflow.BatchSource

/**
 * @author bluejoe2008@gmail.com
 */

case class SeqAsSource[X: Encoder](t: X*) extends BatchSource[X] {
	override def toString = this.getClass.getSimpleName;
	def createDataset(ctx: RunnerContext): Dataset[X] = {
		ctx.forType[SparkSession].createDataset(t);
	}
}