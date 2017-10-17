package cn.piflow.processor

import cn.piflow.JobContext

/**
	* @author bluejoe2008@gmail.com
	*
	*         do nothing, just sleep, this class is for test use
	*/
case class DoSleep(sleepTime: Long) extends Processor020 {
	def perform020() = {
		Thread.sleep(sleepTime);
	}
}