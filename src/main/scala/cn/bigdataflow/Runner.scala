package cn.bigdataflow

import java.util.Date

import scala.collection.JavaConversions.asScalaSet
import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.quartz.impl.StdSchedulerFactory
import org.quartz.Trigger
import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.TriggerBuilder
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.commons.lang.StringUtils
import java.util.Base64
import java.nio.ByteBuffer

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

trait ProcessStatus;

trait Process {
	def parent: Option[Process];
	def getChildren(): Seq[Process];
	def getCurrentStatus(): ProcessStatus;
	def stop();
	def awaitTermination(timeoutMs: Long)
	def awaitTermination();
}

abstract class SingleProcess(runnable: Runnable) extends Process {
	def parent: Option[Process] = None;
	def getChildren() = Seq[Process]();
	def getCurrentStatus(): ProcessStatus;
	def stop();
	def awaitTermination(timeoutMs: Long)
	def awaitTermination();
}

class SparkRunner(spark: SparkSession) extends Logging {

	val scheduler = StdSchedulerFactory.getDefaultScheduler();

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}

abstract	class SimpleJob extends Job {

	}

	def scheduleJob() {
		val trigger =
			TriggerBuilder.newTrigger().withIdentity("trigger1", "group1").startNow().build();

		val jobDetail =
			JobBuilder.newJob(classOf[SimpleJob]).withIdentity("job1", "group1").build();

		scheduler.scheduleJob(jobDetail, trigger);
	}

	def createRunnerContext(): RunnerContext =
		new MapAsRunnerContext(scala.collection.mutable.Map[String, Any](classOf[SparkSession].getName -> spark));

	def run(flowGraph: FlowGraph): Process = {

		//validation
		validate(flowGraph);

		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());
null;
	}

	def runValidatedFlowGrpah(flowGraph: FlowGraph) {
		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());
new Runnable() {
			def run() = {
		val ctx: RunnerContext = createRunnerContext();
		val visitedNodes = scala.collection.mutable.Map[Integer, Map[String, Any]]();
				endNodes.toSeq.foreach(visitNode(flowGraph, _, visitedNodes, ctx));
			}
		};
		scheduleJob();
	}

	private def visitNode(flow: FlowGraph, nodeId: Integer, visitedNodes: scala.collection.mutable.Map[Integer, Map[String, _]], ctx: RunnerContext): Map[String, _] = {
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
				val outputs = visitNode(flow, predecessorNodeId, visitedNodes, ctx);
				inputs += (edgeValue._2 -> outputs(edgeValue._1));
			}

			val outputs = thisNode.processor.performN2N(inputs.toMap, ctx);
			visitedNodes(nodeId) = outputs;
			outputs;
		}
	}
}

object Runner {
	def sparkRunner(spark: SparkSession) = new SparkRunner(spark) {}
}
