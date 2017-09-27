package cn.bigdataflow.shell

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._
import org.apache.commons.io.IOUtils
import scala.xml.XML

object Shell {
	def main(args: Array[String]) {
		Shell.run();
	}

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
		val repl = new ILoop {
			override def createInterpreter() = {
				super.createInterpreter();
				intp.beQuietDuring {
					//intp.bind("spark", spark.getClass.getName, spark);
				}

				val text = IOUtils.toString(this.getClass.getResource("/predefined.scala").openStream());
				intp.quietRun(text.replaceAll("object\\s+PRELOAD_CODES\\s+\\{([\\s\\S]*)\\}", "$1"));
			}

			override def printWelcome(): Unit = {
				println(properties("WelcomeMessage"));
			}

			override def prompt = properties("ShellPrompt");
		}

		val settings = new Settings;
		
		settings.Yreplsync.value = true;
		//use when launching normally outside SBT
		settings.usejavacp.value = true;
		settings.debug.value = false;
		repl.process(settings);
	}
}
