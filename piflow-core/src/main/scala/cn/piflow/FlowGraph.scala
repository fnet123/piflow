package cn.piflow

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.processor.Processor
import cn.piflow.util.FormatUtils
import com.google.common.graph.{EndpointPair, MutableValueGraph, ValueGraphBuilder}

import scala.collection.JavaConversions.asScalaSet

class FlowNode(val id: Int, val processor: Processor) {
}

class FlowGraph {
	val graph: MutableValueGraph[Integer, (String, String)] =
		ValueGraphBuilder.directed().build();
	val nodesMap = collection.mutable.Map[Integer, FlowNode]();
	val nodeId = new AtomicInteger(0);

	def createNode(processor: Processor): FlowNode = {
		val nid = nodeId.incrementAndGet();
		graph.addNode(nid);
		val node = new FlowNode(nid, processor);
		nodesMap(nid) = node;
		node;
	}

	def link(thisNode: FlowNode, thatNode: FlowNode,
					 portNamePair: (String, String) = ("_1", "_1")): FlowGraph = {
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

	def node(id: Integer) = nodesMap(id);
}