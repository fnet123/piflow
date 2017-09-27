package cn.bigdataflow

import java.util.Date

import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import cn.bigdataflow.runner.SparkRunner

trait Logging {
	protected val logger = Logger.getLogger(this.getClass);
}

trait RunnerContext {
	def apply[T](name: String): T;
	def forType[T: ClassTag](implicit m: Manifest[T]): T =
		apply(m.runtimeClass.getName);

	def update[T](name: String, value: T);
}

trait Runner {
	def getJobManager(): JobManager;
	def schedule(flowGraph: FlowGraph, scheduler: Schedule = Schedule.startNow): ScheduledJob;
	/**
	 * run a flow graph until termination
	 */
	def run(flowGraph: FlowGraph, timeout: Long = 0);
	def stop();
}

object Runner {
	def sparkRunner(spark: SparkSession) = SparkRunner;
}

trait JobManager {
	def getFireCount(jobId: JobId): Int;
	def getHistoricExecutions(jobId: JobId): Seq[JobInstance];
	def getScheduledJobs(): Seq[ScheduledJob];
	def getRunningJobs(): Seq[JobInstance];
	def getRunningJobs(jobId: JobId): Seq[JobInstance];
	def resume(jobId: JobId);
	def pause(jobId: JobId);
	def stop(jobId: JobId);
}

trait JobInstance {
	def getId(): String;
	def getScheduledJob(): ScheduledJob;
	def getStartTime(): Date;
	def getRunTime(): Long;
}

trait ScheduledJob {
	def getFlowGraph(): FlowGraph;
	def getId(): JobId;
	def getNextFireTime(): Date;
	def getStartTime(): Date;
	def getEndTime(): Date;
	def getPreviousFireTime(): Date;
}

trait JobId {
	def getId(): String;
}