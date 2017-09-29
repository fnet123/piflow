package cn.piflow.dsl

import cn.piflow.FlowGraph
import cn.piflow.ProcessorNode
import cn.piflow.io.BatchSink
import cn.piflow.processor.Processor
import cn.piflow.processor.io.DoWrite

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
