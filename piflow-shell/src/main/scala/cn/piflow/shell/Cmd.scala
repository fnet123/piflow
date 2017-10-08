package cn.piflow.shell

import org.apache.commons.io.IOUtils

trait Cmd {
	lazy val helpInfo = IOUtils.toString(this.getClass.getResource("/help-info/" +
		this.getClass.getSimpleName + ".txt").openStream());

	def help() = {
		println(helpInfo);
	}
}