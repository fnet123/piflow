package cn.bigdataflow.processors

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import cn.bigdataflow.sql.LabledDatasets
import scala.reflect.ClassTag
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import cn.bigdataflow.io.BucketSink
import cn.bigdataflow.io.BucketSource
import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 */
trait Processor {
	def DEFAULT_IN_PORT_NAMES(n: Int): Seq[String] = {
		(1 to n).map("in:_" + _);
	}

	def DEFAULT_OUT_PORT_NAMES(n: Int): Seq[String] = {
		(1 to n).map("out:_" + _);
	}

	def getInPortNames(): Seq[String];
	def getOutPortNames(): Seq[String];
	def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets;
}

trait Processor121[X, Y] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[Y];
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]], ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor12N[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	def perform(input: Dataset[X], ctx: RunnerContext): LabledDatasets;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		perform(inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]], ctx);
}

trait ProcessorN21[X] extends Processor {
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(inputs: LabledDatasets, ctx: RunnerContext): Dataset[X];
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(inputs, ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor021[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(0);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(ctx: RunnerContext): Dataset[X];
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor02N extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(0);
	def perform(ctx: RunnerContext): LabledDatasets;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets = perform(ctx);
}

trait Processor120[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(0);
	def perform(input: Dataset[X], ctx: RunnerContext): Unit;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets = {
		perform(inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]], ctx);
		Map();
	}
}

case class DoMap[X: Encoder, Y: Encoder](fn: X ⇒ Y) extends Processor121[X, Y] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[Y] = {
		input.map(fn);
	}
}

case class DoFilter[X](fn: X ⇒ Boolean) extends Processor121[X, X] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[X] = {
		input.filter(fn);
	}
}

case class DoFork[X](conditions: X ⇒ Boolean*) extends Processor12N[X] {
	/**
	 * creates N copies
	 */
	def this(ncopy: Int) = { this((1 to ncopy).map(n ⇒ (m: X) ⇒ true): _*) }
	override def toString = this.getClass.getSimpleName;
	def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(conditions.size);
	def perform(input: Dataset[X], ctx: RunnerContext): LabledDatasets = {
		val map = collection.mutable.Map[String, Dataset[Any]]();
		val conditionsMap = getOutPortNames.zip(conditions);
		conditionsMap.foreach { x ⇒
			map(x._1) = input.filter(x._2).asInstanceOf[Dataset[Any]];
		}

		map.toMap
	}
}

case class DoLoad(sources: BucketSource[_]*) extends Processor02N {
	def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(sources.size);
	override def perform(ctx: RunnerContext): LabledDatasets = {
		val map = collection.mutable.Map[String, Dataset[Any]]();
		getOutPortNames().zip(sources.map(_.createDataset(ctx))).foreach { x ⇒
			map(x._1) = x._2.asInstanceOf[Dataset[Any]];
		}

		map.toMap;
	}
}

case class DoWrite[X](sinks: BucketSink[X]*) extends Processor120[X] {
	override def perform(input: Dataset[X], ctx: RunnerContext) {
		sinks.foreach(_.consumeDataset(input, ctx));
	}
}

case class DoZip[X: Encoder, Y: Encoder]()(implicit ct: ClassTag[Y], en: Encoder[(X, Y)]) extends ProcessorN21[(X, Y)] {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);

	def perform(inputs: LabledDatasets, ctx: RunnerContext): Dataset[(X, Y)] = {
		val ds1: Dataset[X] = inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]];
		val ds2: Dataset[Y] = inputs(getInPortNames()(1)).asInstanceOf[Dataset[Y]];
		ds1.sparkSession.createDataset(ds1.rdd.zip(ds2.rdd));
	}
}

case class DoMerge[X: Encoder]() extends ProcessorN21[X] {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);
	def perform(inputs: LabledDatasets, ctx: RunnerContext): Dataset[X] = {
		inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]]
			.union(inputs(getInPortNames()(1)).asInstanceOf[Dataset[X]]);
	}
}