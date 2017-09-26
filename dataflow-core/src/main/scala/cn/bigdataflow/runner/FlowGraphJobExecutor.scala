package cn.bigdataflow.runner

import scala.collection.JavaConversions.asScalaSet
import org.apache.spark.sql.SparkSession

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.Logging
import cn.bigdataflow.RunnerContext
import cn.bigdataflow.processor.ProcessorN2N;
import java.util.concurrent.atomic.AtomicInteger

class MapAsRunnerContext(map: scala.collection.mutable.Map[String, Any]) extends RunnerContext {
	def apply[T](name: String): T = map(name).asInstanceOf[T];
	def update[T](name: String, value: T) = map(name) = value;
}

object FlowGraphExecutor extends Logging {
	lazy val spark = SparkSession.builder.getOrCreate();
	private def createRunnerContext(): RunnerContext =
		new MapAsRunnerContext(scala.collection.mutable.Map[String, Any](classOf[SparkSession].getName -> spark));

	def executeFlowGraph(flowGraph: FlowGraph) {
		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());

		val ctx: RunnerContext = createRunnerContext();
		val visitedNodes = scala.collection.mutable.Map[Integer, Map[String, Any]]();
		endNodes.toSeq.foreach(visitNode(flowGraph, _, visitedNodes, ctx));
	}

	private def visitNode(flow: FlowGraph, nodeId: Integer,
		visitedNodes: scala.collection.mutable.Map[Integer, Map[String, _]],
		ctx: RunnerContext): Map[String, _] = {
		if (visitedNodes.contains(nodeId)) {
			visitedNodes(nodeId);
		}
		else {
			val thisNode = flow.node(nodeId);
			logger.debug(s"visiting node: $nodeId");

			val inputs = collection.mutable.Map[String, Any]();

			//predecessors
			val predecessorNodeIds = flow.graph.predecessors(nodeId);
			for (predecessorNodeId ← predecessorNodeIds) {
				val edgeValue = flow.graph.edgeValue(predecessorNodeId, nodeId).get;
				val outputs = visitNode(flow, predecessorNodeId, visitedNodes, ctx);
				if (edgeValue._1 != null && edgeValue._2 != null)
					inputs += (edgeValue._2 -> outputs(edgeValue._1));
			}

			val outputs = ProcessorN2N.fromUnknown(thisNode.processor).performN2N(inputs.toMap, ctx);
			visitedNodes(nodeId) = outputs;
			outputs;
		}
	}
}