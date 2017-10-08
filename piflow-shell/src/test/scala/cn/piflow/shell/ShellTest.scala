package cn.piflow.shell

;

import org.junit.{Assert, Test}

class ShellTest {
	@Test
	def testRun() = {
		ShellRunner.main(Array[String]());
	}
}