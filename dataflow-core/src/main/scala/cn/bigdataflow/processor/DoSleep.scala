package cn.bigdataflow.processor

import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 *
 * do nothing, just sleep, this class is for test use
 */
case class DoSleep(sleepTime: Long) extends Processor121 {
	def perform(input: Any, ctx: RunnerContext): Any = {
		Thread.sleep(sleepTime);
		input;
	}
}