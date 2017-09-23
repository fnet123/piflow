package cn.bigdataflow;

import java.io.File
import java.io.FileWriter

import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.junit.Test

import cn.bigdataflow.io.ConsoleSink
import cn.bigdataflow.io.MemorySink
import cn.bigdataflow.io.SeqAsSource
import cn.bigdataflow.processor.transform.DoFilter
import cn.bigdataflow.processor.transform.DoFork
import cn.bigdataflow.processor.transform.DoLoad
import cn.bigdataflow.processor.transform.DoMap
import cn.bigdataflow.processor.transform.DoMerge
import cn.bigdataflow.processor.transform.DoWrite
import cn.bigdataflow.processor.transform.DoZip
import cn.bigdataflow.io.SeqAsSource
import java.util.Date
import cn.bigdataflow.io.SeqAsSource

class JobManagerTest {
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	import spark.implicits._

	@Test
	def test() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoMap[Int, Int](_ + 1));
		val mem = ConsoleSink[String]();
		val node3 = fg.createNode(DoWrite(mem));
		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.show();

		val runner = Runner.sparkRunner(spark);

		val jid1 = runner.run(fg);
		val jid2 = runner.run(fg, JobScheduler.startNow());
		val jid3 = runner.run(fg, JobScheduler.startLater(1000));
		val jid4 = runner.run(fg, JobScheduler.startAt(new Date(System.currentTimeMillis() + 2000)));
		val jid5 = runner.run(fg, JobScheduler.startLater(1000).repeatWithInterval(1000));
		val jid6 = runner.run(fg, JobScheduler.startAt(new Date(System.currentTimeMillis() + 2000)).repeatWithInterval(1000));
		val jid7 = runner.run(fg, JobScheduler.startNow().repeatCronly("*/5 * * * *"));

		val man = runner.getJobManager();
		val sjs = man.getScheduledJobs();
		val jobs = man.getRunningJobs();
		Assert.assertEquals(6, sjs.size);
		Assert.assertEquals(1, jobs.size);
	}
}

