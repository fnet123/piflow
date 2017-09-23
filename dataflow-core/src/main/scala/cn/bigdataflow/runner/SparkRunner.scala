package cn.bigdataflow.runner

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.sql.SparkSession
import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobExecutionContext
import org.quartz.TriggerBuilder
import org.quartz.impl.StdSchedulerFactory

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.JobId
import cn.bigdataflow.JobManager
import cn.bigdataflow.JobScheduler
import cn.bigdataflow.Logging
import cn.bigdataflow.Runner
import cn.bigdataflow.JobScheduler

/**
 * @author bluejoe2008@gmail.com
 */
class SparkRunner(spark: SparkSession) extends Runner with Logging {
	val quartzScheduler = StdSchedulerFactory.getDefaultScheduler();
	val jobId = new AtomicInteger(0);

	def getJobManager(): JobManager = {
		new FlowGraphJobManager(quartzScheduler);
	}

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}

	def run(flowGraph: FlowGraph, scheduler: JobScheduler): JobId = {
		//validation
		validate(flowGraph);

		val jobDetail =
			JobBuilder.newJob(classOf[FlowGraphJob])
				.build();
		jobDetail.getJobDataMap.put(classOf[FlowGraph].getName, flowGraph);
		val triggerBuilder = TriggerBuilder
			.newTrigger()
			.withIdentity("" + jobId.incrementAndGet(), classOf[FlowGraph].getName);

		if (scheduler.scheduleBuilder.isDefined)
			triggerBuilder.withSchedule(scheduler.scheduleBuilder.get);

		if (scheduler.startTime.isDefined)
			triggerBuilder.startAt(scheduler.startTime.get);
		else
			triggerBuilder.startNow();

		val trigger = triggerBuilder.build();
		quartzScheduler.scheduleJob(jobDetail, trigger);
		new SimpleJobId(jobDetail.getKey, trigger.getKey);
	}
}

class FlowGraphJob extends Job {
	override def execute(ctx: JobExecutionContext) = {
		val map = ctx.getJobDetail.getJobDataMap;
		val flowGraph = map(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];
		FlowGraphExecutor.executeFlowGraph(flowGraph);
	}
}