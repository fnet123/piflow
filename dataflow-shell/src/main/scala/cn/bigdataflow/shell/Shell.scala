package cn.bigdataflow.shell

import scala.tools.nsc.interpreter._
import scala.tools.nsc.Settings

object Shell {
	def run() {
		val repl = new ILoop {
			override def createInterpreter() = {
				super.createInterpreter();
				intp.bind("author", "String", "bluejoe2008@gmail.com")
			}

			override def printWelcome(): Unit = {
				Console println "yyyyy";
			}

			override def prompt = ">>>";
		}

		val settings = new Settings
		settings.Yreplsync.value = true
		//use when launching normally outside SBT
		settings.usejavacp.value = true
		System.setProperty("scala.repl.prompt", "∏>");
		System.setProperty("scala.repl.welcome", """welcome to pi-data-flow command line interface!
^(*￣(oo)￣)^		  
		  """);
		repl.process(settings)
	}
}
