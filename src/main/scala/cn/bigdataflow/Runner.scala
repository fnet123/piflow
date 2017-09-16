package cn.bigdataflow

import java.util.Date

import scala.collection.JavaConversions.asScalaSet
import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import cn.bigdataflow.Processor.LabledDatasets

trait Logging {
	protected val logger = Logger.getLogger(this.getClass);
}

trait RunnerContext {
	def apply[T](name: String): T;
	def forType[T: ClassTag](implicit m: Manifest[T]): T =
		apply(m.runtimeClass.getName);

	def update[T](name: String, value: T);
}

class MapAsRunnerContext(map: scala.collection.mutable.Map[String, Any]) extends RunnerContext {
	def apply[T](name: String): T = map(name).asInstanceOf[T];
	def update[T](name: String, value: T) = map(name) = value;
}

trait Runner {
	def listener(): RunningEventListener;

	def run(flow: Flow);
	def schedule(flow: Flow, date: Date) {
	}

	def schedule(flow: Flow, cronExpr: String) {
	}
}

class SparkRunner(spark: SparkSession, listener: RunningEventListener) extends Runner with Logging {

	def listener(): RunningEventListener = listener;
	val ctx: RunnerContext = new MapAsRunnerContext(scala.collection.mutable.Map[String, Any](classOf[SparkSession].getName -> spark));

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}

	def run(flow: Flow) {
		run(flow.flow);
	}

	def run(flowGraph: FlowGraph) {
		//validation
		validate(flowGraph);
		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());
		val visitedNodes = scala.collection.mutable.Map[Integer, Map[String, Any]]();
		endNodes.toSeq.foreach(visitNode(flowGraph, _, visitedNodes));
	}

	private def visitNode(flow: FlowGraph, nodeId: Integer, visitedNodes: scala.collection.mutable.Map[Integer, LabledDatasets]): LabledDatasets = {
		if (visitedNodes.contains(nodeId)) {
			visitedNodes(nodeId);
		}
		else {
			val thisNode = flow.node(nodeId);
			logger.debug(s"running node: $nodeId");

			val inputs = collection.mutable.Map[String, Any]();

			//predecessors
			val predecessorNodeIds = flow.graph.predecessors(nodeId);
			for (predecessorNodeId ← predecessorNodeIds) {
				val edgeValue = flow.graph.edgeValue(predecessorNodeId, nodeId).get;
				val outputs = visitNode(flow, predecessorNodeId, visitedNodes);
				inputs += (edgeValue._2 -> outputs(edgeValue._1));
			}

			val outputs = thisNode.processor.performN2N(inputs.toMap, ctx);
			visitedNodes(nodeId) = outputs;
			outputs;
		}
	}
}

object Runner {
	def sparkRunner(spark: SparkSession, listener: RunningEventListener = new NullRunningEventListener()) = new SparkRunner(spark, listener) {}
}
