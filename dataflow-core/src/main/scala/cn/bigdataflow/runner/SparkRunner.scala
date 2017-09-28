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
import cn.bigdataflow.JobManager
import cn.bigdataflow.Logging
import cn.bigdataflow.Runner
import cn.bigdataflow.Schedule
import cn.bigdataflow.ScheduledJob
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Lock
import org.quartz.TriggerKey

/**
 * @author bluejoe2008@gmail.com
 */
object SparkRunner extends Runner with Logging {
	val quartzScheduler = StdSchedulerFactory.getDefaultScheduler();
	val teg: TriggerExtraGroup = new TriggerExtraGroupImpl();
	val schedulerListener = new SchedulerListenerImpl(teg);
	quartzScheduler.getListenerManager.addSchedulerListener(schedulerListener);
	//quartzScheduler.getListenerManager.addJobListener(new FlowGraphJobListener());
	val triggerListener = new TriggerListenerImpl(teg);
	val jobManager = new JobManagerImpl(quartzScheduler, teg);
	quartzScheduler.getListenerManager.addTriggerListener(triggerListener);
	quartzScheduler.start();

	//val startTime = System.currentTimeMillis() / 1000;
	val jobId = new AtomicInteger(0);

	def getJobManager(): JobManager = jobManager;

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}

	def stop = {
		if (!jobManager.getScheduledJobs().isEmpty)
			logger.warn("Runner is to be shutdown, while there are running jobs!");

		quartzScheduler.shutdown();
	}

	def run(flowGraph: FlowGraph, timeout: Long = 0): ScheduledJob = {
		val sj = schedule(flowGraph).asInstanceOf[ScheduledJobImpl];
		val key = sj.trigger.getKey;

		teg.get(key).awaitTermination(if (timeout > 0) { timeout } else { 0 });

		if (timeout > 0 && quartzScheduler.getTrigger(key) != null)
			quartzScheduler.unscheduleJob(key);

		sj;
	}

	def schedule(flowGraph: FlowGraph, scheduler: Schedule): ScheduledJob = {
		//validation
		validate(flowGraph);

		val jobDetail =
			JobBuilder.newJob(classOf[FlowGraphJob])
				.build();
		jobDetail.getJobDataMap.put(classOf[FlowGraph].getName, flowGraph);

		val triggerId = "" + jobId.incrementAndGet();
		val triggerBuilder = TriggerBuilder
			.newTrigger()
			.withIdentity(triggerId, classOf[FlowGraph].getName);

		if (scheduler.scheduleBuilder.isDefined)
			triggerBuilder.withSchedule(scheduler.scheduleBuilder.get);

		if (scheduler.startTime.isDefined)
			triggerBuilder.startAt(scheduler.startTime.get);
		else
			triggerBuilder.startNow();

		val trigger = triggerBuilder.build();

		quartzScheduler.scheduleJob(jobDetail, trigger);
		new ScheduledJobImpl(jobDetail, trigger);
	}
}

class FlowGraphJob extends Job with Logging {
	override def execute(ctx: JobExecutionContext) = {
		val map = ctx.getJobDetail.getJobDataMap;
		val flowGraph = map(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];
		JobExecutor.executeFlowGraph(flowGraph);
	}
}