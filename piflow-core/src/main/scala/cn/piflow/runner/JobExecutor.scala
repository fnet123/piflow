package cn.piflow.runner

import cn.piflow.processor.ProcessorN2N
import cn.piflow.{FlowException, FlowGraph, Logging, RunnerContext}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.asScalaSet

class JobExecutionException(currentNodeId: Integer, predecessorNodeId: Integer, ports: (String, String), cause: Throwable)
	extends FlowException(s"fail to execute job, currentNodeId=$currentNodeId, predecessorNodeId=$predecessorNodeId, ports=$ports",
		cause) {

}

class MapAsRunnerContext(map: scala.collection.mutable.Map[String, Any]) extends RunnerContext {
	def apply[T](name: String): T = map(name).asInstanceOf[T];

	def update[T](name: String, value: T) = map(name) = value;

	override def isDefined(name: String): Boolean = map.contains(name);
}

object JobExecutor extends Logging {
	lazy val spark = SparkSession.builder.getOrCreate();

	def executeFlowGraph(flowGraph: FlowGraph) {
		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());
		logger.debug(s"end nodes: $endNodes");

		val ctx: RunnerContext = createRunnerContext();
		//TODO: analyze flow graph
		ctx("isStreaming") = false;
		val visitedNodes = scala.collection.mutable.Map[Integer, Map[String, Any]]();
		endNodes.toSeq.foreach(visitNode(flowGraph, _, visitedNodes, ctx));
	}

	private def createRunnerContext(): RunnerContext =
		new MapAsRunnerContext(scala.collection.mutable.Map[String, Any](classOf[SparkSession].getName -> spark));

	private def visitNode(flow: FlowGraph, nodeId: Integer,
												visitedNodes: scala.collection.mutable.Map[Integer, Map[String, _]],
												ctx: RunnerContext): Map[String, _] = {
		if (visitedNodes.contains(nodeId)) {
			visitedNodes(nodeId);
		}
		else {
			val thisNode = flow.node(nodeId);
			val inputs = collection.mutable.Map[String, Any]();

			//predecessors
			val predecessorNodeIds = flow.graph.predecessors(nodeId);
			for (predecessorNodeId ← predecessorNodeIds) {
				val edgeValue = flow.graph.edgeValue(predecessorNodeId, nodeId).get;
				try {
					val outputs = visitNode(flow, predecessorNodeId, visitedNodes, ctx);
					if (edgeValue._1 != null && edgeValue._2 != null) {
						inputs += (edgeValue._2 -> outputs(edgeValue._1));
					}
				}
				catch {
					case e: Throwable => throw new JobExecutionException(nodeId, predecessorNodeId, edgeValue, e);
				}
			}

			val processor = thisNode.processor;
			logger.debug(s"visiting node: $processor, node id: $nodeId");
			val outputs = ProcessorN2N.fromUnknown(processor).performN2N(inputs.toMap, ctx);
			logger.debug(s"visited node: $processor, node id: $nodeId");
			visitedNodes(nodeId) = outputs;
			outputs;
		}
	}
}