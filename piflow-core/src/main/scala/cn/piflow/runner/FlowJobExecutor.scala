package cn.piflow.runner

import cn.piflow._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.quartz.JobExecutionContext

import scala.collection.JavaConversions.asScalaSet
import scala.reflect.ClassTag

class FlowJobExecutionException(nodeId: Integer, proccesor: Processor, cause: Throwable)
	extends FlowException(s"fail to execute job, nodeId=$nodeId, processor=$proccesor", cause) {

}

object FlowJobExecutor extends Logging {
	lazy val spark = SparkSession.builder.getOrCreate();

	def executeFlowGraph(flowGraph: FlowGraph, jobManager: FlowJobManagerImpl, jec: JobExecutionContext) {

		val ctx: JobContext = new JobContext() {

			val map = collection.mutable.Map[String, Any](
				"checkpointLocation" -> spark.conf.get("spark.sql.streaming.checkpointLocation")
			);

			def notifyEvent(event: FlowGraphEvent) = jobManager.receive(jobInstanceId, event);

			def sparkSession(): SparkSession = spark;

			def sqlContext(): SQLContext = spark.sqlContext;

			def flowGraph(): FlowGraph = flowGraph;

			def jobInstanceId: String = jec.getFireInstanceId;


			def arg[T](name: String): T = map(name).asInstanceOf[T];

			def arg[T](name: String, value: T) = {
				map(name) = value;
				this;
			}

			def notify(message: FlowGraphEvent) = {
				//TODO: async
				jobManager.receive(jobInstanceId, message);
			}
		}

		//TODO: analyze flow graph
		//initializing all nodes first
		for ((nodeId, flowNode) <- flowGraph.nodesMap) {
			val pctx = new ProcessorContext {
				override def arg[T](name: String) = ctx.arg(name);

				override def notifyEvent(event: FlowGraphEvent) = ctx.notifyEvent(event);

				override def sparkSession = ctx.sparkSession;

				override def sqlContext: SQLContext = ctx.sqlContext;

				override def argForType[T: ClassTag](implicit m: Manifest[T]): T = ctx.argForType[T];

				override def arg[T](name: String, value: T) = {
					ctx.arg(name, value);
					this;
				}

				override def flowGraph: FlowGraph = ctx.flowGraph;

				override def jobInstanceId: String = ctx.jobInstanceId;

				def jobContext: JobContext = ctx;

				def flowNodeId: Int = nodeId;
			};

			val processor = flowNode.processor;
			processor.init(pctx);
		};

		val visitedNodes = scala.collection.mutable.Map[Integer, Map[String, Any]]();
		val endNodes = flowGraph.graph.nodes().filter(flowGraph.graph.successors(_).isEmpty());
		logger.debug(s"end nodes: $endNodes");
		endNodes.toSeq.foreach(visitNode(flowGraph, _, visitedNodes, ctx));
	}

	private def visitNode(flow: FlowGraph, nodeId: Integer,
	                      visitedNodes: scala.collection.mutable.Map[Integer, Map[String, _]],
	                      ctx: JobContext): Map[String, _] = {
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
				processor.notifyEvent(ProcessorStarted());
				val outputs = processor.performN2N(inputs.toMap);
				processor.notifyEvent(ProcessorCompleted());
				logger.debug(s"visited node: $processor, node id: $nodeId");
				visitedNodes(nodeId) = outputs;
				outputs;
			}
			catch {
				case e: Throwable => {
					processor.notifyEvent(ProcessorFailed());
					throw new FlowJobExecutionException(nodeId, processor, e);
				}
			}
		}
	}
}