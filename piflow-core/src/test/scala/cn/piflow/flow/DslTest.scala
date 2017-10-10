package cn.piflow.flow

import cn.piflow.Runner
import cn.piflow.dsl._
import cn.piflow.io.{MemorySink, SeqAsSource}
import cn.piflow.processor.DoSleep
import cn.piflow.processor.ds._
import cn.piflow.processor.io.{DoWrite, DoLoad}
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class DslTest {
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

	import spark.implicits._

	implicit val runner = Runner.sparkRunner(spark);

	@Test
	def testSingleNodeFlow() = {
		val line1 = DoSleep(3000);
		val ref1 = ProcessorRef();
		val line2 = DoSleep(3000) % ref1

		line1.show();
		runner.run(line1);

		line2.show();
		runner.run(line2);
		Assert.assertEquals(classOf[DoSleep], ref1.processor.getClass);
	}

	@Test
	def testFlowSequence() {
		val ref1 = SinkRef();

		val line1 = SeqAsSource(1, 2, 3, 4) > "_1:_1" >
			DoMap[Int, Int](_ + 1) >
			MemorySink() % ref1;

		line1.show();
		runner.run(line1);
		Assert.assertEquals(Seq(2, 3, 4, 5), ref1.as[MemorySink].as[Int]);
		Assert.assertEquals(ref1.as[MemorySink], ref1.get);
		Assert.assertEquals(classOf[DoWrite], ref1.processor.getClass);

		val line2 = SeqAsSource(1, 2, 3, 4) >
			DoMap[Int, Int](_ + 1) >
			MemorySink();

		line2.show();
		runner.run(line2);
	}

	@Test
	def testFlowSequence2() = {
		val ref1 = SourceRef();
		val ref2 = ProcessorRef();
		val ref3 = SinkRef();

		val line = SeqAsSource(1, 2, 3, 4) % ref1 > "_1:_1" >
			DoMap[Int, Int](_ + 1) % ref2 >
			MemorySink() % ref3;

		runner.run(line);
		Assert.assertEquals(SeqAsSource(1, 2, 3, 4), ref1.get);
		Assert.assertEquals(Seq(2, 3, 4, 5), ref3.as[MemorySink].as[Int]);
		Assert.assertEquals(ref3.as[MemorySink], ref3.as[MemorySink]);
		Assert.assertEquals(classOf[DoLoad], ref1.processor.getClass);
		Assert.assertEquals(classOf[DoMap[Int, Int]], ref2.processor.getClass);
		Assert.assertEquals(classOf[DoWrite], ref3.processor.getClass);
	}

	@Test
	def testFlowFork() = {
		val ref1 = SinkRef();
		val ref2 = SinkRef();
		val forkNode = ProcessorRef();

		val line = SeqAsSource(1, 2, 3, 4) >
			DoFork[Int](_ % 2 == 0, _ % 2 == 1) % forkNode >
			MemorySink() % ref1;

		forkNode > "_2:_1" > MemorySink() % ref2;

		val runner = Runner.sparkRunner(spark);
		line.show();
		runner.run(line);
		Assert.assertEquals(Seq(2, 4), ref1.as[MemorySink].as[Int]);
		Assert.assertEquals(Seq(1, 3), ref2.as[MemorySink].as[Int]);
	}

	@Test
	def testFlowMerge() = {
		val mem = SinkRef();
		val zipNode = ProcessorRef();

		val line1 = SeqAsSource(1, 2, 3, 4) >
			DoMap[Int, Int](_ + 10) >
			DoZip[Int, String]() % zipNode >
			MemorySink() % mem;

		val line2 = SeqAsSource("a", "b", "c", "d") >
			DoMap[String, String](_.toUpperCase()) > "_1:_2" >
			zipNode;

		val runner = Runner.sparkRunner(spark);
		line1.show();
		runner.run(line1);
		Assert.assertEquals(Seq(Seq(11, "A"), Seq(12, "B"), Seq(13, "C"), Seq(14, "D")), mem.as[MemorySink].asSeq);
	}

	@Test
	def testFlowMerge2() = {
		val mem = SinkRef();
		val zipNode = ProcessorRef();

		val line1 = SeqAsSource(1, 2, 3, 4) >
			DoMap[Int, Int](_ + 10);

		val line2 = SeqAsSource("a", "b", "c", "d") >
			DoMap[String, String](_.toUpperCase());

		val line3 = Seq(line1 > "_1:_1", line2 > "_1:_2") >
			DoZip[Int, String]() >
			MemorySink() % mem;

		val runner = Runner.sparkRunner(spark);
		line3.show();
		runner.run(line3);
		Assert.assertEquals(Seq(Seq(11, "A"), Seq(12, "B"), Seq(13, "C"), Seq(14, "D")),
			mem.as[MemorySink].asSeq);
	}

	@Test
	def testFlowForkMerge() = {
		val mem = SinkRef();
		val mergeNode = ProcessorRef();

		val line1 = SeqAsSource(1, 2, 3, 4) >
			DoFork[Int](_ % 2 == 0, _ % 2 == 1) >
			DoMap[Int, String](x ⇒ (x + 10).toString()) >
			DoMerge[String]() % mergeNode >
			MemorySink() % mem;

		SeqAsSource("a", "b", "c", "d") >
			DoMap[String, String](_.toUpperCase()) >
			(DoFilter[String](_.charAt(0) <= 'B') > "_1:_2") >
			mergeNode;

		val runner = Runner.sparkRunner(spark);
		line1.show();
		runner.run(line1);

		Assert.assertEquals(Seq("12", "14", "A", "B"), mem.as[MemorySink].as[String]);
	}
}

