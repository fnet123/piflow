package cn.piflow

import cn.piflow.dsl._
import cn.piflow.io.{MemorySink, SeqAsSource}
import cn.piflow.processor.ds._
import cn.piflow.processor.io.{DoWrite, _DoLoadSource}
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class DslTest {
  val spark = SparkSession.builder.master("local[4]")
    .getOrCreate();
  spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

  import spark.implicits._

  implicit val runner = Runner.sparkRunner(spark);

  @Test
  def testFlowSequence() = {
    val mem1 = Ref();
    val mem2 = Ref();
    val node = NodeRef();

    val line = SeqAsSource(1, 2, 3, 4) / "_1:_1" >
      DoMap[Int, Int](_ + 1) >
      MemorySink() % node % mem1 % mem2;

    line.show();
    runner.run(line);
    Assert.assertEquals(Seq(2, 3, 4, 5), mem1.as[MemorySink].as[Int]);
    Assert.assertEquals(mem1.as[MemorySink], mem2.as[MemorySink]);
    Assert.assertEquals(classOf[DoWrite], node.get.processorNode.processor.getClass);
  }

  @Test
  def testFlowSequence2() = {
    val source = Ref();
    val mem1 = Ref();
    val mem2 = Ref();
    val node1 = NodeRef();
    val node2 = NodeRef();
    val node3 = NodeRef();

    val line = SeqAsSource(1, 2, 3, 4) % source / "_1:_1" >
      DoMap[Int, Int](_ + 1) % node2 >
      MemorySink() % node3 % mem1 % mem2;

    runner.run(line);
    Assert.assertEquals(SeqAsSource(1, 2, 3, 4), source.get);
    Assert.assertEquals(Seq(2, 3, 4, 5), mem1.as[MemorySink].as[Int]);
    Assert.assertEquals(mem1.as[MemorySink], mem2.as[MemorySink]);
    Assert.assertEquals(classOf[_DoLoadSource], node1.get.processorNode.processor.getClass);
    Assert.assertEquals(classOf[DoMap[Int, Int]], node2.get.processorNode.processor.getClass);
    Assert.assertEquals(classOf[DoWrite], node3.get.processorNode.processor.getClass);
  }

  @Test
  def testFlowFork() = {
    val mem1 = Ref();
    val mem2 = Ref();
    val forkNode = NodeRef();

    val line = SeqAsSource(1, 2, 3, 4) >
      DoFork[Int](_ % 2 == 0, _ % 2 == 1) % forkNode >
      MemorySink() % mem1;

    forkNode / "_2:_1" > MemorySink() % mem2;

    val runner = Runner.sparkRunner(spark);
    line.show();
    runner.run(line);
    Assert.assertEquals(Seq(2, 4), mem1.as[Int]);
    Assert.assertEquals(Seq(1, 3), mem2.as[Int]);
  }

  @Test
  def testFlowMerge() = {
    val mem = Ref();
    val zipNode = NodeRef();

    val line1 = SeqAsSource(1, 2, 3, 4) >
      DoMap[Int, Int](_ + 10) >
      DoZip[Int, String]() % zipNode >
      MemorySink() % mem;

    val line2 = SeqAsSource("a", "b", "c", "d") >
      DoMap[String, String](_.toUpperCase()) / "_1:_2" >
      zipNode;

    val runner = Runner.sparkRunner(spark);
    runner.run(line1);
    Assert.assertEquals(Seq(Seq(11, "A"), Seq(12, "B"), Seq(13, "C"), Seq(14, "D")), mem.as[MemorySink].asSeq);
  }

  @Test
  def testFlowMerge2() = {
    val mem = Ref();
    val zipNode = NodeRef();

    val line1 = SeqAsSource(1, 2, 3, 4) >
      DoMap[Int, Int](_ + 10) >
      DoZip[Int, String]();

    val line2 = SeqAsSource("a", "b", "c", "d") >
      DoMap[String, String](_.toUpperCase());

    val line3 = Seq(line1 / "_1:_1", line2 / "_1:_2") >
      DoZip[Int, String]() >
      MemorySink() % mem;

    val runner = Runner.sparkRunner(spark);
    runner.run(line3);
    Assert.assertEquals(Seq(Seq(11, "A"), Seq(12, "B"), Seq(13, "C"), Seq(14, "D")), mem.as[MemorySink].asSeq);
  }

  @Test
  def testFlowForkMerge() = {
    val mem = Ref();
    val mergeNode = NodeRef();

    val line1 = SeqAsSource(1, 2, 3, 4) >
      DoFork[Int](_ % 2 == 0, _ % 2 == 1) >
      DoMap[Int, String](x ⇒ (x + 10).toString()) >
      DoMerge[String]() % mergeNode >
      MemorySink();

    SeqAsSource("a", "b", "c", "d") >
      DoMap[String, String](_.toUpperCase()) >
      (DoFilter[String](_.charAt(0) <= 'B') / "_1:_2") >
      mergeNode;

    val runner = Runner.sparkRunner(spark);
    runner.run(line1);

    Assert.assertEquals(Seq("12", "14", "A", "B"), mem.as[String]);
  }
}

