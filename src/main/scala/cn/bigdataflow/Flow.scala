package cn.bigdataflow

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder

import cn.bigdataflow.lib.processors.DoFilter
import cn.bigdataflow.lib.processors.DoLoad
import cn.bigdataflow.lib.processors.DoWrite
import cn.bigdataflow.lib.processors.Processor121

/**
 * operator of FlowGraph
 */
class Flow(val flow: FlowGraph, var currentNode: ProcessorNode) {
	def pipe[X, Y](transform: Processor121[X, Y], lables: (String, String)): Flow = {
		this.append(transform, lables);
	}

	def write[X: Encoder](sink: BatchSink[X], lables: (String, String)): Flow = {
		this.append(new DoWrite(sink), lables);
	}

	def fork[X: Encoder](lastEdgeLable: String, filters: (String, X ⇒ Boolean)*): Seq[Flow] = {
		filters.map(fn ⇒ {
			val transform: Processor121[Dataset[X], Dataset[X]] = new DoFilter[X](fn._2);
			pipe(transform, (lastEdgeLable, fn._1));
		})
	}

	def append(processor: Processor, lables: (String, String)): Flow = {
		val newNode = flow.createNode(processor);
		flow.link(currentNode, newNode, lables);
		new Flow(flow, currentNode);
	}
}

class FlowBuilder(val flow: FlowGraph) {
	def beginWith[X: Encoder](source: BatchSource[X]): Flow = {
		new Flow(flow, flow.createNode(DoLoad[X](source)));
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