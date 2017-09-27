package cn.bigdataflow.runner

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import org.quartz.JobExecutionContext
import org.quartz.TriggerKey

trait TriggerExtraGroup {
	def get(key: TriggerKey): TriggerExtra;
	def login(key: TriggerKey);
	def logout(key: TriggerKey);
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
	val triggers = collection.mutable.Map[TriggerKey, TriggerExtra]();
	def get(key: TriggerKey): TriggerExtra = triggers(key);

	def logout(key: TriggerKey) {
		triggers.remove(key);
	}

	def login(key: TriggerKey) = {
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