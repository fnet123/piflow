package cn.bigdataflow.lib.processors

import org.apache.spark.sql.Dataset

import cn.bigdataflow.Processor
import cn.bigdataflow.Processor.LabledDatasets
import cn.bigdataflow.RunnerContext

/**
 * @author bluejoe2008@gmail.com
 */
trait Processor121[X, Y] extends Processor {
	override def getInPortNames(): Seq[String] = Processor.DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = Processor.DEFAULT_OUT_PORT_NAMES(1);
	def perform(input: X, ctx: RunnerContext): Y;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx).asInstanceOf[Y]);
}

trait Processor12N[X] extends Processor {
	override def getInPortNames(): Seq[String] = Processor.DEFAULT_IN_PORT_NAMES(1);
	def perform(input: X, ctx: RunnerContext): LabledDatasets;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx);
}

trait ProcessorN21[X] extends Processor {
	override def getOutPortNames(): Seq[String] = Processor.DEFAULT_OUT_PORT_NAMES(1);
	def perform(inputs: LabledDatasets, ctx: RunnerContext): X;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(inputs, ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor021[X] extends Processor {
	override def getInPortNames(): Seq[String] = Processor.DEFAULT_IN_PORT_NAMES(0);
	override def getOutPortNames(): Seq[String] = Processor.DEFAULT_OUT_PORT_NAMES(1);
	def perform(ctx: RunnerContext): X;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets =
		Map(getOutPortNames()(0) -> perform(ctx).asInstanceOf[Dataset[Any]]);
}

trait Processor02N extends Processor {
	override def getInPortNames(): Seq[String] = Processor.DEFAULT_IN_PORT_NAMES(0);
	def perform(ctx: RunnerContext): LabledDatasets;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets = perform(ctx);
}

trait Processor120[X] extends Processor {
	override def getInPortNames(): Seq[String] = Processor.DEFAULT_IN_PORT_NAMES(1);
	override def getOutPortNames(): Seq[String] = Processor.DEFAULT_OUT_PORT_NAMES(0);
	def perform(input: X, ctx: RunnerContext): Unit;
	override def performN2N(inputs: LabledDatasets, ctx: RunnerContext): LabledDatasets = {
		perform(inputs(getInPortNames()(0)).asInstanceOf[X], ctx);
		Map();
	}
}