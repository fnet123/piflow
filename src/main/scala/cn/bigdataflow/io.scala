package cn.bigdataflow

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

trait Source[X];

trait BatchSource[X] extends Source[X] {
	def createDataset(ctx: RunnerContext): Dataset[X];
}

trait StreamSource[X] extends Source[X] {

}

trait Sink[X];

trait BatchSink[X] extends Sink[X] {
	def consumeDataset(ds: Dataset[X], ctx: RunnerContext);
}

trait StreamSink[X] extends Sink[X] {
	def consumeDataset(ds: Dataset[X], ctx: RunnerContext): StreamingQuery;
}