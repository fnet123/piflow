package cn.bigdataflow

import org.apache.spark.sql.Encoder

import cn.bigdataflow.io.BatchSource
import cn.bigdataflow.dsl.PipedProcessorNode
import cn.bigdataflow.processor.Processor
import cn.bigdataflow.processor.transform.DoLoad

package object dsl {
	implicit def piped[T: Encoder](source: BatchSource[T]): PipedProcessorNode = {
		piped(DoLoad(source));
	}

	implicit def piped[T: Encoder](processor: Processor): PipedProcessorNode = {
		val flowGraph = new FlowGraph();
		val currentNode = flowGraph.createNode(processor);
		new PipedProcessorNode(flowGraph, currentNode);
	}

	implicit def toGraph(node: PipedProcessorNode): FlowGraph = node.flowGraph;
}