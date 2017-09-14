package cn.bigdataflow.processors

import org.apache.spark.sql.Dataset

import cn.bigdataflow.Processor
import cn.bigdataflow.RunnerContext
import cn.bigdataflow.sql.LabledDatasets

/**
 * @author bluejoe2008@gmail.com
 */
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