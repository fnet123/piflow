package cn.bigdataflow.processor

import org.apache.spark.sql.Dataset

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
}

trait ProcessorN2N {
	def getInPortNames(): Seq[String];
	def getOutPortNames(): Seq[String];
	def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _];
}

trait Processor121[X, Y] extends Processor {
	def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);
	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);
	def perform(input: X, ctx: RunnerContext): Y;
}

trait Processor12N[X] extends Processor {
	def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);
	def getOutPortNames(): Seq[String];
	def perform(input: X, ctx: RunnerContext): Map[String, _];
}

trait ProcessorN21[X] extends Processor {
	def getInPortNames(): Seq[String];
	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);
	def perform(inputs: Map[String, _], ctx: RunnerContext): X;
}

trait Processor021[X] extends Processor {
	def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);
	def perform(ctx: RunnerContext): X;
}

trait Processor02N extends Processor {
	def getOutPortNames(): Seq[String];
	def perform(ctx: RunnerContext): Map[String, _];
}

trait Processor020 extends Processor {
	def perform(ctx: RunnerContext): Unit;
}

trait Processor120[X] extends Processor {
	def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);
	def perform(input: X, ctx: RunnerContext): Unit;
}

trait ProcessorN20 extends Processor {
	def getInPortNames(): Seq[String];
	def perform(inputs: Map[String, _], ctx: RunnerContext): Unit;
}

class WrongProcessorException(processor: Processor) extends RuntimeException(
	String.format("wrong processor type of %s, expected: %s", processor.toString(), Seq(
		classOf[ProcessorN2N], classOf[Processor120[_]], classOf[Processor021[_]], classOf[Processor020], classOf[Processor121[_, _]],
		classOf[Processor12N[_]], classOf[ProcessorN21[_]], classOf[Processor02N], classOf[ProcessorN20]))) {
}

object ProcessorN2N {
	def fromUnknown(processor: Processor): ProcessorN2N = {
		processor match {
			case x: Processor120[_] ⇒ from(x);
			case x: Processor121[_, _] ⇒ from(x);
			case x: Processor021[_] ⇒ from(x);
			case x: Processor020 ⇒ from(x);
			case x: Processor12N[_] ⇒ from(x);
			case x: ProcessorN21[_] ⇒ from(x);
			case x: ProcessorN2N ⇒ x;
			case _ ⇒ throw new WrongProcessorException(processor);
		}
	}

	def from[X](processor: Processor120[X]): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());
		override def getOutPortNames(): Seq[String] = Seq();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
			processor.perform(inputs(processor.getInPortName()).asInstanceOf[X], ctx);
			Map();
		}
	}

	def from[X, Y](processor: Processor121[X, Y]): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());
		override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			Map(processor.getOutPortName() -> processor.perform(inputs(processor.getInPortName()).asInstanceOf[X], ctx).asInstanceOf[Y]);
	}

	def from[Y](processor: Processor021[Y]): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq();
		override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			Map(processor.getOutPortName() -> processor.perform(ctx).asInstanceOf[Y]);
	}

	def from[X](processor: Processor12N[X]): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());
		override def getOutPortNames(): Seq[String] = processor.getOutPortNames();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			processor.perform(inputs(processor.getInPortName()).asInstanceOf[X], ctx);
	}

	def from(processor: Processor02N): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq();
		override def getOutPortNames(): Seq[String] = processor.getOutPortNames();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			processor.perform(ctx);
	}

	def from[Y](processor: ProcessorN21[Y]): ProcessorN2N = new ProcessorN2N {
		override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());
		override def getInPortNames(): Seq[String] = processor.getInPortNames();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			Map(processor.getOutPortName() -> processor.perform(inputs, ctx).asInstanceOf[Y]);
	}

	def from(processor: ProcessorN20): ProcessorN2N = new ProcessorN2N {
		override def getOutPortNames(): Seq[String] = Seq();
		override def getInPortNames(): Seq[String] = processor.getInPortNames();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
			{
				processor.perform(inputs, ctx);
				Map();
			}
	}

	def from(processor: Processor020): ProcessorN2N = new ProcessorN2N {
		override def getInPortNames(): Seq[String] = Seq();
		override def getOutPortNames(): Seq[String] = Seq();
		override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
			processor.perform(ctx);
			Map();
		}
	}
}