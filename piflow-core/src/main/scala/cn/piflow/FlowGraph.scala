package cn.piflow

import cn.piflow.processor.Processor
import cn.piflow.util.FormatUtils
import com.google.common.graph.{EndpointPair, MutableValueGraph, ValueGraphBuilder}

import scala.collection.JavaConversions.asScalaSet

class ProcessorNode(val id: Int, val processor: Processor) {
}

class FlowGraph {
	val graph: MutableValueGraph[Integer, (String, String)] = ValueGraphBuilder.directed().build();
	val nodesMap = collection.mutable.Map[Integer, ProcessorNode]();
	var serialNo = 0;

	def node(id: Integer) = nodesMap(id);

	def createNode(processor: Processor): ProcessorNode = {
		serialNo += 1;
		graph.addNode(serialNo);
		val node = new ProcessorNode(serialNo, processor);
		nodesMap(serialNo) = node;
		node;
	}

	def link(thisNode: ProcessorNode, thatNode: ProcessorNode, portNamePair: (String, String) = ("out:_1", "in:_1")): FlowGraph = {
		graph.putEdgeValue(thisNode.id, thatNode.id, portNamePair);
		this;
	}

	def show() {
		val data = graph.edges().toSeq
			.map { pair: EndpointPair[Integer] ⇒
				val startNodeId = pair.source();
				val endNodeId = pair.target();
				val startNode = node(startNodeId).processor.toString();
				val endNode = node(endNodeId).processor.toString();
				val (lable1, lable2) = graph.edgeValue(startNodeId, endNodeId).get;
				Seq[Any](s"$startNodeId->$endNodeId", s"$startNode", lable1, lable2, s"$endNode");
			}

		FormatUtils.printTable(Seq("", "source node", "out port", "in port", "target node"), data);
	}
}