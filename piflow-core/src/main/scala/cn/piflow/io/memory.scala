package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
	* @author bluejoe2008@gmail.com
	*/
case class MemorySink() extends BatchSink with StreamSink {
	var _sparkMemorySink: SparkMemorySink = null;
	var _recoverFromCheckpointLocation: Boolean = false;
	var _outputMode: OutputMode = null;

	override def destroy(): Unit = {

	}

	override def saveBatch(ds: Dataset[_]) = {
		addBatch(-1, ds.toDF());
	}

	override def addBatch(batchId: Long, data: DataFrame): Unit = {
		if (_sparkMemorySink == null) {
			val rows = data.head(1);
			if (!rows.isEmpty) {
				val row = rows(0);
				_sparkMemorySink = new SparkMemorySink(row.schema, _outputMode);
			}
		}

		_sparkMemorySink.addBatch(batchId, data);
	}

	override def init(outputMode: OutputMode, ctx: RunnerContext) = {
		_outputMode = outputMode;
		_recoverFromCheckpointLocation = ctx.isDefined("checkpointLocation") &&
			outputMode == OutputMode.Complete();
	}

	override def toString = this.getClass.getSimpleName;

	override def useTempCheckpointLocation(): Boolean = true;

	override def recoverFromCheckpointLocation(): Boolean = _recoverFromCheckpointLocation;


	def as[T]: Seq[_] = asRow.map {
		_.apply(0).asInstanceOf[T];
	}

	def asSeq: Seq[Seq[_]] = asRow.map {
		_.toSeq;
	}

	def asRow: Seq[Row] = {
		if (_sparkMemorySink != null)
			_sparkMemorySink.allData;
		else
			Seq[Row]();
	};
}