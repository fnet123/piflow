package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
	* @author bluejoe2008@gmail.com
	*/
case class ConsoleSink() extends SparkStreamSinkAdapter
	with BatchSink with StreamSink {
	override def destroy(): Unit = {

	}

	def createSparkStreamSink(outputMode: OutputMode, ctx: RunnerContext) = new SparkConsoleSink(Map[String, String]());

	override def toString = this.getClass.getSimpleName;

	override def init(outputMode: OutputMode, ctx: RunnerContext): Unit = {

	}

	def saveBatch(ds: Dataset[_]) = {
		ds.show();
	}

	override def useTempCheckpointLocation(): Boolean = true;

	override def recoverFromCheckpointLocation(): Boolean = false;
}