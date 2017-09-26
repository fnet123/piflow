package cn.bigdataflow.pipe

import cn.bigdataflow.io.BatchSink
import cn.bigdataflow.ProcessorNode
import cn.bigdataflow.processor.transform.DoWrite
import cn.bigdataflow.FlowGraph
import cn.bigdataflow.processor.Processor
import org.apache.spark.sql.Encoder
import cn.bigdataflow.Runner
import java.util.Date
import cn.bigdataflow.Schedule

/**
 * @author bluejoe2008@gmail.com
 */
class PipedProcessorNode(val flowGraph: FlowGraph, val currentNode: ProcessorNode) {
	def >(successor: Processor): PipedProcessorNode = {
		val successorNode = flowGraph.createNode(successor);
		flowGraph.link(currentNode, successorNode);
		new PipedProcessorNode(flowGraph, successorNode);
	}

	def >[T: Encoder](sink: BatchSink[T]): PipedProcessorNode = {
		>(DoWrite[T](sink));
	}

	def >>[T: Encoder](sink: BatchSink[T]): PipedProcessorNode = {
		>(DoWrite[T](sink));
	}

	def !()(implicit runner: Runner) = {
		runner.run(flowGraph);
	}

	def !@(date: Date)(implicit runner: Runner) = {
		runner.schedule(flowGraph, Schedule.startAt(date));
	}

	def !@(schedule: Schedule)(implicit runner: Runner) = {
		runner.schedule(flowGraph, schedule);
	}

	def !@(cronExpression: String)(implicit runner: Runner) = {
		runner.schedule(flowGraph, Schedule.startNow().repeatCronly(cronExpression));
	}

	def !@(delay: Long)(implicit runner: Runner) = {
		runner.schedule(flowGraph, Schedule.startLater(delay));
	}
}
