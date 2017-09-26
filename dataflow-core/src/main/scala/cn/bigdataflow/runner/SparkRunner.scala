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
class SparkRunner(spark: SparkSession) extends Runner with Logging {
	val quartzScheduler = StdSchedulerFactory.getDefaultScheduler();
	val schedulerListener = new FlowGraphJobSchedulerListener();
	quartzScheduler.getListenerManager.addSchedulerListener(schedulerListener);
	//quartzScheduler.getListenerManager.addJobListener(new FlowGraphJobListener());
	quartzScheduler.getListenerManager.addTriggerListener(new FlowGraphJobTriggerListener());
	quartzScheduler.start();

	val jobId = new AtomicInteger(0);

	def getJobManager(): JobManager = {
		new FlowGraphJobManager(quartzScheduler);
	}

	private def validate(flowGraph: FlowGraph) {
		//ports
		//no-loop
	}

	def stop = {
		quartzScheduler.shutdown();
	}

	def run(flowGraph: FlowGraph, timeout: Long = 0) {
		val sj = schedule(flowGraph).asInstanceOf[SimpleScheduledJob];
		val key = sj.trigger.getKey;
		val lock = schedulerListener.getLock(key);
		lock.synchronized {
			lock.wait(if (timeout > 0) { timeout } else { 0 });
		}

		if (timeout > 0 && quartzScheduler.getTrigger(key) != null)
			quartzScheduler.unscheduleJob(key);
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
		new SimpleScheduledJob(jobDetail, trigger);
	}
}

class FlowGraphJob extends Job with Logging {
	override def execute(ctx: JobExecutionContext) = {
		val map = ctx.getJobDetail.getJobDataMap;
		val flowGraph = map(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];
		FlowGraphExecutor.executeFlowGraph(flowGraph);
	}
}