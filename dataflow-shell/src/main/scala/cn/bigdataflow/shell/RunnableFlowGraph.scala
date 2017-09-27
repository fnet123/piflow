package cn.bigdataflow.shell

import java.util.Date

import cn.bigdataflow.Runner
import cn.bigdataflow.Schedule
import cn.bigdataflow.dsl.PipedProcessorNode
import cn.bigdataflow.FlowGraph

/**
 * @author bluejoe2008@gmail.com
 */

class RunnableFlowGraph(flowGraph: FlowGraph)(implicit runner: Runner) {
	def this(node: PipedProcessorNode)(implicit runner: Runner) = this(node.flowGraph)(runner);

	def !() = {
		runner.run(flowGraph);
	}

	def &() = {
		runner.schedule(flowGraph);
	}

	def &(date: Date) = {
		runner.schedule(flowGraph, Schedule.startAt(date));
	}

	def &(schedule: Schedule) = {
		runner.schedule(flowGraph, schedule);
	}

	def &(cronExpression: String) = {
		runner.schedule(flowGraph, Schedule.startNow().repeatCronly(cronExpression));
	}

	def &(delay: Long) = {
		runner.schedule(flowGraph, Schedule.startLater(delay));
	}
}