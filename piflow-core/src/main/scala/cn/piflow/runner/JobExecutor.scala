package cn.piflow.runner

import cn.piflow._
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.JavaConversions.asScalaSet

class JobExecutionException(nodeId: Integer, proccesor: Processor, cause: Throwable)
	extends FlowException(s"fail to execute job, nodeId=$nodeId, processor=$proccesor", cause) {

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
		new MapAsRunnerContext(scala.collection.mutable.Map[String, Any](
			classOf[SparkSession].getName -> spark,
			classOf[SQLContext].getName -> spark.sqlContext,
			"checkpointLocation" -> spark.conf.get("spark.sql.streaming.checkpointLocation")
		));

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
				val outputs = visitNode(flow, predecessorNodeId, visitedNodes, ctx);
				if (edgeValue._1 != null && edgeValue._2 != null) {
					inputs += (edgeValue._2 -> outputs(edgeValue._1));
				}
			}

			val processor = thisNode.processor;
			logger.debug(s"visiting node: $processor, node id: $nodeId");

			try {
				val outputs = processor.performN2N(inputs.toMap, ctx);
				logger.debug(s"visited node: $processor, node id: $nodeId");
				visitedNodes(nodeId) = outputs;
				outputs;
			}
			catch {
				case e: Throwable => throw new JobExecutionException(nodeId, processor, e);
			}
		}
	}
}