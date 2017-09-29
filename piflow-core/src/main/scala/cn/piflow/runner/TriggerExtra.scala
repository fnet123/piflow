package cn.piflow.runner

import java.util.concurrent.atomic.AtomicInteger

import org.quartz.{JobExecutionContext, TriggerKey}

import scala.collection.mutable.ArrayBuffer

trait TriggerExtra {
	def notifyTermination(): Unit;
	def awaitTermination(timeout: Long = 0): Unit;
	def increaseFireCount(): Unit;
	def getFireCount(): Int;
}

trait TriggerExtraGroup {
	def getHistoricExecutions(): Seq[JobExecutionContext];
	def appendExecution(ctx: JobExecutionContext);
	def get(key: String): TriggerExtra;
	def get(triggerKey: TriggerKey): TriggerExtra = get(triggerKey.getName);
	def getOrCreate(key: String): TriggerExtra;
	def getOrCreate(triggerKey: TriggerKey): TriggerExtra = getOrCreate(triggerKey.getName);
}

class TriggerExtraGroupImpl(maxHistorySize: Int = 102400) extends TriggerExtraGroup {
	val triggers = collection.mutable.Map[String, TriggerExtra]();
	val historicExecutions = ArrayBuffer[JobExecutionContext]();

	var dirtyFlag = 0;
	val dirtyThreshold = 100;
	def get(key: String): TriggerExtra = {
		triggers.synchronized {
			triggers(key);
		}
	}

	def getHistoricExecutions(): Seq[JobExecutionContext] = historicExecutions;

	def appendExecution(ctx: JobExecutionContext) = {
		historicExecutions += ctx;

		dirtyFlag += 1;
		if (dirtyFlag >= dirtyThreshold) {
			if (maxHistorySize >= 0 && historicExecutions.size >= maxHistorySize)
				historicExecutions.synchronized {
					historicExecutions.trimStart(historicExecutions.size - maxHistorySize);
				}

			dirtyFlag = 0;
		}
	}

	def getOrCreate(key: String) = {
		triggers.getOrElseUpdate(key, new TriggerExtra() {
			val termination = new Object();
			val counter = new AtomicInteger(0);

			def notifyTermination() = {
				termination.synchronized {
					termination.notify();
				}
			}

			def awaitTermination(timeout: Long) = {
				termination.synchronized {
					termination.wait();
				}
			}

			def increaseFireCount() = {
				counter.incrementAndGet();
			}

			def getFireCount() = counter.get;
		});
	}
}