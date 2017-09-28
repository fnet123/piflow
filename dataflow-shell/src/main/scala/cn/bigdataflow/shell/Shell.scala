package cn.bigdataflow.shell

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop
import scala.xml.XML

import org.apache.commons.io.IOUtils
import javax.script.ScriptContext
import cn.bigdataflow.Runner

class Shell {
	lazy val properties = {
		//load properties from properties.xml
		val xml = XML.loadString(IOUtils.toString(this.getClass.getResource("/properties.xml").openStream()));
		val elements = xml \ "property";
		val map = collection.mutable.Map[String, String]();
		elements.map { x ⇒
			map(x.attribute("name").get.text) = x.attribute("value").getOrElse(x).text;
		}
		map;
	}

	def run() {
		val repl: ILoop = new ILoop {
			override def createInterpreter() = {
				super.createInterpreter();
				val text = IOUtils.toString(this.getClass.getResource("/predefined.scala").openStream());
				intp.quietRun(text.replaceAll("object\\s+PRELOAD_CODES\\s+\\{([\\s\\S]*)\\}", "$1"));
				intp.beQuietDuring {
					//intp.bind("jobs", new cn.bigdataflow.shell.cmd.JobCmd(runner));
					//intp.bind("store", new cn.bigdataflow.shell.cmd.StoreCmd(runner));
				}
			}

			override def printWelcome(): Unit = {
				println(properties("WelcomeMessage"));
			}

			val promptMsg = properties("ShellPrompt");
			override def prompt = promptMsg;
		}

		val settings = new Settings;

		settings.Yreplsync.value = true;
		//use when launching normally outside SBT
		settings.usejavacp.value = true;
		settings.debug.value = false;
		repl.process(settings);
		//FIXME: do not stop!!		
		Runner.sparkRunner(null).stop();
	}
}
