package cn.piflow

import org.apache.spark.sql.Encoder
import cn.piflow.dsl.PipedProcessorNode
import cn.piflow.io.BatchSource
import cn.piflow.processor.Processor
import cn.piflow.processor.io.DoLoad

package object dsl {
	implicit def piped(source: BatchSource): PipedProcessorNode = {
		piped(DoLoad(source));
	}

	implicit def piped(processor: Processor): PipedProcessorNode = {
		val flowGraph = new FlowGraph();
		val currentNode = flowGraph.createNode(processor);
		new PipedProcessorNode(flowGraph, currentNode);
	}

	implicit def toGraph(node: PipedProcessorNode): FlowGraph = node.flowGraph;
}