package cn.piflow

import java.util.concurrent.atomic.AtomicInteger
import cn.piflow.runner.{ProcessorEvent, EventFromProcessor}
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
				Seq[Any](startNodeId -> endNodeId, s"$startNodeId->$endNodeId", startNode, lable1, lable2, endNode)
			}.sortBy(_.apply(0).asInstanceOf[(Int, Int)]).map(_.drop(1));

		FormatUtils.printTable(Seq("", "source node", "out port", "in port", "target node"), data);
	}

	def node(id: Integer) = nodesMap(id);
}

trait Processor {
	protected var _processorContext: ProcessorContext = null;

	def context = _processorContext;

	def notifyEvent(event: ProcessorEvent) = _processorContext.notifyEvent(
		EventFromProcessor(_processorContext.flowNodeId, event));

	def init(ctx: ProcessorContext): Unit = {
		_processorContext = ctx;
	}

	def getInPortNames(): Seq[String];

	def getOutPortNames(): Seq[String];

	def performN2N(inputs: Map[String, _]): Map[String, _];

	def DEFAULT_IN_PORT_NAMES(n: Int): Seq[String] = {
		(1 to n).map("_" + _);
	}

	def DEFAULT_OUT_PORT_NAMES(n: Int): Seq[String] = {
		(1 to n).map("_" + _);
	}

	def destroy(): Unit = {}
}