package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode

trait Source;

trait BatchSource extends Source {
  def createDataset(ctx: RunnerContext): Dataset[_];
}

trait StreamSource extends Source {

}

trait Sink;

trait BatchSink extends Sink {
  def saveDataset(ds: Dataset[_], outputMode: OutputMode, ctx: RunnerContext): Unit;
}

trait StreamSink extends Sink {
  def addBatch(batchId: Long, data: DataFrame, outputMode: OutputMode, ctx: RunnerContext): Unit;

  def useTempCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext): Boolean = false;

  def recoverFromCheckpointLocation(outputMode: OutputMode, ctx: RunnerContext) = true;
}