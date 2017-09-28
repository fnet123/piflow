package cn.bigdataflow.runner

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.quartz.JobExecutionContext
import org.quartz.TriggerKey

trait TriggerExtraGroup {
	def get(key: String): TriggerExtra;
	def get(triggerKey: TriggerKey): TriggerExtra = get(triggerKey.getName);
	def login(key: String): Unit;
	def login(triggerKey: TriggerKey): Unit = login(triggerKey.getName);
	def logout(key: String): Unit;
	def logout(triggerKey: TriggerKey): Unit = logout(triggerKey.getName);
}

trait TriggerExtra {
	def notifyTermination(): Unit;
	def awaitTermination(timeout: Long = 0): Unit;
	def increaseFireCount(): Unit;
	def getFireCount(): Int;
	def getHistoricExecutions(): Seq[JobExecutionContext];
	def appendExecution(ctx: JobExecutionContext);
}

class TriggerExtraGroupImpl(maxHistorySize: Int = 10240) extends TriggerExtraGroup {
	val triggers = collection.mutable.Map[String, TriggerExtra]();

	def get(key: String): TriggerExtra = {
		triggers.synchronized {
			triggers(key);
		}
	}

	def logout(key: String) {
		triggers.synchronized {
			triggers.remove(key);
		}
	}

	def login(key: String) = {
		triggers.synchronized {
			triggers(key) = new TriggerExtra() {
				val termination = new Object();
				val counter = new AtomicInteger(0);
				var dirtyFlag = 0;
				val dirtyThreshold = 100;
				val historicExecutions = ArrayBuffer[JobExecutionContext]();

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
			}
		}
	}
}