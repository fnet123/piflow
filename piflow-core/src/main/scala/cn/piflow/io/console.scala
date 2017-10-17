package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
	* @author bluejoe2008@gmail.com
	*/
case class ConsoleSink() extends SparkStreamSinkAdapter
	with BatchSink with StreamSink {
	def createSparkStreamSink(outputMode: OutputMode, ctx: JobContext) = new SparkConsoleSink(Map[String, String]());

	override def toString = this.getClass.getSimpleName;

	def writeBatch(ds: Dataset[_]): Unit = {
		ds.show();
	}
}