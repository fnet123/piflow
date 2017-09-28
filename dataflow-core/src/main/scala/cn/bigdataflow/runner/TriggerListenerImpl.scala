package cn.bigdataflow.runner

import org.quartz.JobExecutionContext
import org.quartz.Trigger.CompletedExecutionInstruction
import org.quartz.TriggerListener
import cn.bigdataflow.Logging
import org.quartz.Trigger

/**
 * @author bluejoe2008@gmail.com
 */
class TriggerListenerImpl(teg: TriggerExtraGroup) extends TriggerListener with Logging {
	def getName() = this.getClass.getName;

	def triggerFired(trigger: Trigger, context: JobExecutionContext) = {
		teg.get(trigger.getKey).increaseFireCount();
		logger.debug(String.format("job fired: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}

	def vetoJobExecution(trigger: Trigger, context: JobExecutionContext) = {
		false;
	}

	def triggerMisfired(trigger: Trigger) = {
	}

	def triggerComplete(trigger: Trigger, context: JobExecutionContext,
		triggerInstructionCode: CompletedExecutionInstruction) {
		teg.get(trigger.getKey).appendExecution(context);
		logger.debug(String.format("job completed: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}
}