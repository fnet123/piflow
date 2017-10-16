package cn.piflow.processor

import cn.piflow.{Processor, FlowException, RunnerContext}

/**
	* @author bluejoe2008@gmail.com
	*/

trait Processor121 extends Processor {
	override final def getInPortNames(): Seq[String] = Seq(getInPortName());

	override final def getOutPortNames(): Seq[String] = Seq(getOutPortName());

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortName() ->
			perform121(inputs(getInPortName()), ctx));

	def getInPortName() = DEFAULT_IN_PORT_NAMES(1).head;

	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1).head;

	def perform121(input: Any, ctx: RunnerContext): Any;
}

trait Processor12N extends Processor {
	override final def getInPortNames(): Seq[String] = Seq(getInPortName());

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		perform12N(inputs(getInPortName()), ctx);

	def getInPortName() = DEFAULT_IN_PORT_NAMES(1).head;

	def getOutPortNames(): Seq[String];

	def perform12N(input: Any, ctx: RunnerContext): Map[String, _];
}

trait ProcessorN21 extends Processor {
	override final def getOutPortNames(): Seq[String] = Seq(getOutPortName());

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortName() -> performN21(inputs, ctx));

	def getInPortNames(): Seq[String];

	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1).head;

	def performN21(inputs: Map[String, _], ctx: RunnerContext): Any;
}

trait Processor021 extends Processor {
	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1).head;

	def perform021(ctx: RunnerContext): Any;

	override final def getInPortNames(): Seq[String] = Seq();

	override final def getOutPortNames(): Seq[String] = Seq(getOutPortName());

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		Map(getOutPortName() -> perform021(ctx));
}

trait Processor02N extends Processor {
	def getOutPortNames(): Seq[String];

	def perform02N(ctx: RunnerContext): Map[String, _];

	override final def getInPortNames(): Seq[String] = Seq();

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
		perform02N(ctx);
}

trait Processor020 extends Processor {
	override final def getInPortNames(): Seq[String] = Seq();

	override final def getOutPortNames(): Seq[String] = Seq();

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
		perform020(ctx);
		Map();
	}

	def perform020(ctx: RunnerContext): Unit;
}

trait Processor120 extends Processor {
	def getInPortName() = DEFAULT_IN_PORT_NAMES(1).head;

	def perform120(input: Any, ctx: RunnerContext): Unit;

	override final def getInPortNames(): Seq[String] = Seq(getInPortName());

	override final def getOutPortNames(): Seq[String] = Seq();

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
		perform120(inputs(getInPortName()), ctx);
		Map();
	}
}

trait ProcessorN20 extends Processor {
	override final def getOutPortNames(): Seq[String] = Seq();

	override final def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
		performN20(inputs, ctx);
		Map();
	}

	def getInPortNames(): Seq[String];

	def performN20(inputs: Map[String, _], ctx: RunnerContext): Unit;
}