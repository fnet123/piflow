package cn.piflow;

import java.util.Date

import cn.piflow.io.{ConsoleSink, SeqAsSource}
import cn.piflow.processor.Processor020
import cn.piflow.processor.ds.DoMap
import cn.piflow.processor.io.{DoLoad, DoWrite}
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

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
		val node3 = fg.createNode(DoWrite(ConsoleSink()));
		val node4 = fg.createNode(new Processor020() {
			override def perform(ctx: RunnerContext) = {
				Thread.sleep(3000);
			}
		});

		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.link(node3, node4, (null, null));
		fg.show();

		val runner = Runner.sparkRunner(spark);
		val man = runner.getJobManager();

		Assert.assertEquals(0, man.getScheduledJobs().size);
		Assert.assertEquals(0, man.getRunningJobs().size);

		runner.run(fg); //await termination
		runner.run(fg, 2000); //await termination, timeout=2s

		val sj1 = runner.schedule(fg);
		val sj2 = runner.schedule(fg, Schedule.startNow());
		val sj3 = runner.schedule(fg, Schedule.startLater(1000));
		val sj4 = runner.schedule(fg, Schedule.startAt(new Date(System.currentTimeMillis() + 2000)));
		val sj5 = runner.schedule(fg, Schedule.startLater(1000).repeatWithInterval(1000));
		val sj6 = runner.schedule(fg, Schedule.startAt(new Date(System.currentTimeMillis() + 2000)).repeatWithInterval(1000));
		val sj7 = runner.schedule(fg, Schedule.startNow().repeatDaily(13, 0));
		val sj8 = runner.schedule(fg, Schedule.startNow().repeatCronly("* * * * * ?"));

		Thread.sleep(100000); //0.5s
		runner.stop();
	}
}

