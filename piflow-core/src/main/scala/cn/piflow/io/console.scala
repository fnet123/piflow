package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ConsoleSink => SparkConsoleSink, MemorySink => SparkMemorySink}
import org.apache.spark.sql.streaming.OutputMode

/**
  * @author bluejoe2008@gmail.com
  */
case class ConsoleSink() extends BatchSink with StreamSink {
  val scsMap = Map[String, String]();
  val scs = new SparkConsoleSink(scsMap);

  override def toString = this.getClass.getSimpleName;

  def saveDataset(ds: Dataset[_], outputMode: OutputMode, ctx: RunnerContext) = {
    ds.show();
  }

  def addBatch(batchId: Long, data: DataFrame, outputMode: OutputMode, ctx: RunnerContext): Unit = {
    scs.addBatch(batchId, data);
  }

  override def useTempCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext): Boolean = true;

  override def recoverFromCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext): Boolean = false;
}