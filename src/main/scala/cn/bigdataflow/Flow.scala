package cn.bigdataflow

import org.apache.spark.sql.Encoder
import cn.bigdataflow.processors.Processor121
import cn.bigdataflow.processors.Processor
import cn.bigdataflow.processors.DoWrite
import cn.bigdataflow.processors.DoLoad
import cn.bigdataflow.processors.DoFilter
import cn.bigdataflow.io.BucketSource
import cn.bigdataflow.io.BucketSink

/**
 * operator of FlowGraph
 */
class Flow(val flow: FlowGraph, var currentNode: ProcessorNode) {
	def pipe[X: Encoder, Y: Encoder](transform: Processor121[X, Y], lables: (String, String)): Flow = {
		this.append(transform, lables);
	}

	def write[X: Encoder](sink: BucketSink[X], lables: (String, String)): Flow = {
		this.append(new DoWrite(sink), lables);
	}

	def fork[X: Encoder](lastEdgeLable: String, filters: (String, X ⇒ Boolean)*): Seq[Flow] = {
		filters.map(fn ⇒
			pipe(new DoFilter(fn._2), (lastEdgeLable, fn._1)));
	}

	def append(processor: Processor, lables: (String, String)): Flow = {
		val newNode = flow.createNode(processor);
		flow.link(currentNode, newNode, lables);
		new Flow(flow, currentNode);
	}
}

class FlowBuilder(val flow: FlowGraph) {
	def beginWith[X](source: BucketSource[X]): Flow = {
		new Flow(flow, flow.createNode(new DoLoad(source)));
	}

	def beginWith(processor: Processor): Flow = {
		new Flow(flow, flow.createNode(processor));
	}
}

//flow controller, flow factory
object Flow {
	def batching() = new Flow(new FlowGraph(), null);
	def streaming() = new Flow(new FlowGraph(), null);
}