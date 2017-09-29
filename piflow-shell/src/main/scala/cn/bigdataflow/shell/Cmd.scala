package cn.bigdataflow.shell

import org.apache.commons.io.IOUtils

trait Cmd {
	val helpInfo = IOUtils.toString(this.getClass.getResource("/help-info/" + this.getClass.getSimpleName + ".txt").openStream());
	def help() = {
		println(helpInfo);
	}
}