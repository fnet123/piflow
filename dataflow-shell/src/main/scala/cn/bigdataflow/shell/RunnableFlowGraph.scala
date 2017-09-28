package cn.bigdataflow.shell

import java.util.Date

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.Runner
import cn.bigdataflow.Schedule
import cn.bigdataflow.ScheduledJob
import cn.bigdataflow.dsl.PipedProcessorNode
import cn.bigdataflow.util.FormatUtils

/**
 * @author bluejoe2008@gmail.com
 */

class RunnableFlowGraph(flowGraph: FlowGraph)(implicit runner: Runner) {
	def this(node: PipedProcessorNode)(implicit runner: Runner) = this(node.flowGraph)(runner);

	def !() {
		val time1 = System.currentTimeMillis();
		val job = runner.run(flowGraph);
		val time2 = System.currentTimeMillis();
		val jobId = job.getId();
		val cost = time2 - time1;
		println(s"job complete: id=$jobId, time cost=${cost}ms");
	}

	def &() {
		val job = runner.schedule(flowGraph);
		val jobId = job.getId();
		println(s"job scheduled: id=$jobId");
	}

	def !@(date: Date) = {
		val job = runner.schedule(flowGraph, Schedule.startAt(date));
		printScheduledJobInfo(job);
	}

	def !@(schedule: Schedule) = {
		val job = runner.schedule(flowGraph, schedule);
		printScheduledJobInfo(job);
	}

	def !@(cronExpression: String) = {
		val job = runner.schedule(flowGraph, Schedule.startNow().repeatCronly(cronExpression));
		printScheduledJobInfo(job);
	}

	private def printScheduledJobInfo(job: ScheduledJob) = {
		val jobId = job.getId();
		val nftime = FormatUtils.formatDate(job.getNextFireTime());
		println(s"job scheduled: id=$jobId, next fired time=$nftime");
	}

	def !@(delay: Long) = {
		val job = runner.schedule(flowGraph, Schedule.startLater(delay));
		printScheduledJobInfo(job);
	}
}