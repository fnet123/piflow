package cn.piflow.runner

import cn.piflow.Logging
import org.quartz.Trigger.CompletedExecutionInstruction
import org.quartz.{JobExecutionContext, Trigger, TriggerListener}

/**
 * @author bluejoe2008@gmail.com
 */
class TriggerListenerImpl(teg: TriggerExtraGroup) extends TriggerListener with Logging {
	def getName() = this.getClass.getName;

	def triggerFired(trigger: Trigger, context: JobExecutionContext) = {
		teg.getOrCreate(trigger.getKey).increaseFireCount();
		logger.debug(String.format("job fired: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}

	def vetoJobExecution(trigger: Trigger, context: JobExecutionContext) = {
		false;
	}

	def triggerMisfired(trigger: Trigger) = {
	}

	def triggerComplete(trigger: Trigger, context: JobExecutionContext,
		triggerInstructionCode: CompletedExecutionInstruction) {
		teg.appendExecution(context);
		logger.debug(String.format("job completed: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}
}