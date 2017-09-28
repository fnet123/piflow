package cn.bigdataflow.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery
import cn.bigdataflow.RunnerContext

trait Source;

trait BatchSource extends Source {
	def createDataset(ctx: RunnerContext): Dataset[_];
}

trait StreamSource extends Source {

}

trait Sink;

trait BatchSink extends Sink {
	def consumeDataset(ds: Dataset[_], ctx: RunnerContext);
}

trait StreamSink extends Sink {
	def consumeDataset(ds: Dataset[_], ctx: RunnerContext): StreamingQuery;
}