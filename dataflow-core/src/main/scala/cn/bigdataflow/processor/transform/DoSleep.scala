package cn.bigdataflow.processor.transform

import cn.bigdataflow.RunnerContext
import cn.bigdataflow.processor.Processor121

/**
 * @author bluejoe2008@gmail.com
 *
 * do nothing, just sleep, this class is for test use
 */
case class DoSleep[X](sleepTime: Long) extends Processor121[X, X] {
	def perform(input: X, ctx: RunnerContext): X = {
		Thread.sleep(sleepTime);
		input;
	}
}