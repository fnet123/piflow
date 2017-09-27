package cn.bigdataflow.dsl

import cn.bigdataflow.io.BatchSink
import cn.bigdataflow.ProcessorNode
import cn.bigdataflow.processor.transform.DoWrite
import cn.bigdataflow.FlowGraph
import cn.bigdataflow.processor.Processor
import org.apache.spark.sql.Encoder
import cn.bigdataflow.Runner
import java.util.Date
import cn.bigdataflow.Schedule
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

	def >[T: Encoder](sink: BatchSink[T]): PipedProcessorNode = {
		>(DoWrite[T](sink));
	}

	def >>[T: Encoder](sink: BatchSink[T]): PipedProcessorNode = {
		>(DoWrite[T](sink));
	}
}
