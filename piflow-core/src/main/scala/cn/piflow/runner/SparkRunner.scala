package cn.piflow.runner

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow._
import org.quartz.impl.StdSchedulerFactory
import org.quartz.{Job, JobBuilder, JobExecutionContext, TriggerBuilder}

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

	def stop = {
		if (jobManager.getScheduledJobs().nonEmpty)
			logger.warn("Runner is to be shutdown, while there are running jobs!");

		quartzScheduler.shutdown();
	}

	def run(flowGraph: FlowGraph, timeout: Long = 0): ScheduledJob = {
		val sj = schedule(flowGraph).asInstanceOf[ScheduledJobImpl];
		val key = sj.trigger.getKey;

		teg.get(key).awaitTermination(if (timeout > 0) {
			timeout
		} else {
			0
		});

		if (timeout > 0 && quartzScheduler.getTrigger(key) != null)
			quartzScheduler.unscheduleJob(key);

		sj;
	}

	def schedule(flowGraph: FlowGraph, start: Start.Builder = Start.now, run: Repeat.Builder = Repeat.once): ScheduledJob = {
		val js = new JobSchedule();
		start(js);
		run(js);
		schedule(flowGraph, js);
	}

	def schedule(flowGraph: FlowGraph, schedule: JobSchedule): ScheduledJob = {
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

		if (schedule.scheduleBuilder.isDefined)
			triggerBuilder.withSchedule(schedule.scheduleBuilder.get);

		if (schedule.startTime.isDefined)
			triggerBuilder.startAt(schedule.startTime.get);
		else
			triggerBuilder.startNow();

		val trigger = triggerBuilder.build();

		quartzScheduler.scheduleJob(jobDetail, trigger);
		new ScheduledJobImpl(jobDetail, trigger);
	}

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}
}

class FlowGraphJob extends Job with Logging {
	override def execute(ctx: JobExecutionContext) = {
		val map = ctx.getJobDetail.getJobDataMap;
		val flowGraph = map(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];
		JobExecutor.executeFlowGraph(flowGraph);
	}
}