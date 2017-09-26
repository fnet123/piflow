package cn.bigdataflow.pipe

import org.apache.spark.sql.Encoder

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.ProcessorNode
import cn.bigdataflow.io.BatchSink
import cn.bigdataflow.io.BatchSource
import cn.bigdataflow.processor.Processor
import cn.bigdataflow.processor.transform.DoLoad
import cn.bigdataflow.processor.transform.DoWrite
import cn.bigdataflow.Runner
import java.util.Date
import cn.bigdataflow.Schedule

object Conversions {
	implicit def piped[T: Encoder](source: BatchSource[T]): PipedProcessorNode = {
		piped(DoLoad(source));
	}

	implicit def piped[T: Encoder](processor: Processor): PipedProcessorNode = {
		val flowGraph = new FlowGraph();
		val currentNode = flowGraph.createNode(processor);
		new PipedProcessorNode(flowGraph, currentNode);
	}

	implicit def toGraph(node: PipedProcessorNode) = node.flowGraph;
}