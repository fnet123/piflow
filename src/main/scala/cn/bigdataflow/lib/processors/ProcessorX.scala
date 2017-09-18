package cn.bigdataflow.lib.processors

import org.apache.spark.sql.Dataset

import cn.bigdataflow.Processor
import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 */
trait Processor121[X, Y] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(input: X, ctx: RunnerContext): Y;
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortNames()(0) -> perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx).asInstanceOf[Y]);
}

trait Processor12N[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	def perform(input: X, ctx: RunnerContext): Map[String, _];
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx);
}

trait ProcessorN21[X] extends Processor {
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(inputs: Map[String, _], ctx: RunnerContext): X;
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortNames()(0) -> perform(inputs, ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor021[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(0);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(1);
	def perform(ctx: RunnerContext): X;
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortNames()(0) -> perform(ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor02N extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(0);
	def perform(ctx: RunnerContext): Map[String, _];
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = perform(ctx);
}

trait Processor120[X] extends Processor {
	override def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(0);
	def perform(input: X, ctx: RunnerContext): Unit;
	override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
		perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx);
		Map();
	}
}