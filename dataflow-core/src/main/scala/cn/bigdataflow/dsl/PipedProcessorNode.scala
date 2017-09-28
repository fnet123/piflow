package cn.bigdataflow.dsl

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.ProcessorNode
import cn.bigdataflow.io.BatchSink
import cn.bigdataflow.processor.Processor
import cn.bigdataflow.processor.transform.DoWrite

/**
 * @author bluejoe2008@gmail.com
 */
class PipedProcessorNode(val flowGraph: FlowGraph, val currentNode: ProcessorNode) {
	def >(successor: Processor): PipedProcessorNode = {
		val successorNode = flowGraph.createNode(successor);
		flowGraph.link(currentNode, successorNode);
		new PipedProcessorNode(flowGraph, successorNode);
	}

	def >(sink: BatchSink): PipedProcessorNode = {
		>(DoWrite(sink));
	}

	def >>(sink: BatchSink): PipedProcessorNode = {
		>(DoWrite(sink));
	}
}
