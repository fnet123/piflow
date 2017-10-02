package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
  * @author bluejoe2008@gmail.com
  */
case class MemorySink(outputMode: OutputMode = OutputMode.Append) extends BatchSink with StreamSink {
  var optionSparkMemorySink: Option[SparkMemorySink] = None;

  override def toString = this.getClass.getSimpleName;

  override def useTempCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext): Boolean = true;

  override def recoverFromCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext): Boolean = {
    ctx.isDefined("checkpointLocation") && outputMode == OutputMode.Complete()
  }

  override def saveDataset(ds: Dataset[_], outputMode: OutputMode, ctx: RunnerContext) = {
    addBatch(-1, ds.toDF(), outputMode, ctx);
  }

  override def addBatch(batchId: Long, data: DataFrame, outputMode: OutputMode, ctx: RunnerContext): Unit = {
    if (!optionSparkMemorySink.isDefined) {
      val rows = data.head(1);
      if (!rows.isEmpty) {
        val row = rows(0);
        optionSparkMemorySink = Some(new SparkMemorySink(row.schema, outputMode));
      }
    }

    optionSparkMemorySink.foreach(_.addBatch(batchId, data));
  }

  def as[T]: Seq[_] = asRow.map {
    _.apply(0).asInstanceOf[T];
  }

  def asRow: Seq[Row] = {
    if (optionSparkMemorySink.isDefined)
      optionSparkMemorySink.get.allData;
    else
      Seq[Row]();
  };

  def asSeq: Seq[Seq[_]] = asRow.map {
    _.toSeq;
  }
}