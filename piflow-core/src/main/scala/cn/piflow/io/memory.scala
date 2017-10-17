package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
	* @author bluejoe2008@gmail.com
	*/
case class MemorySink() extends SparkStreamSinkAdapter
	with BatchSink with StreamSink {
	def createSparkStreamSink(outputMode: OutputMode, ctx: JobContext) = new SparkMemorySink(null, outputMode);

	override def writeBatch(ds: Dataset[_]) = {
		writeBatch(-1, ds.toDF());
	}

	override def toString = this.getClass.getSimpleName;

	def as[T]: Seq[_] = asRows.map {
		_.apply(0).asInstanceOf[T];
	}

	def asSeqs: Seq[Seq[_]] = asRows.map {
		_.toSeq;
	}

	def asRows: Seq[Row] = {
		if (_sparkStreamSink != null)
			_sparkStreamSink.asInstanceOf[SparkMemorySink].allData;
		else
			Seq[Row]();
	};
}