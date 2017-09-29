package cn.piflow.runner

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.{FlowGraph, JobManager, Logging, Runner, Schedule, ScheduledJob}
import org.quartz.{Job, JobBuilder, JobExecutionContext, TriggerBuilder}
import org.quartz.impl.StdSchedulerFactory

import scala.collection.JavaConversions.mapAsScalaMap

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