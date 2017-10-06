package cn.piflow.processor

import cn.piflow.RunnerContext

/**
  * @author bluejoe2008@gmail.com
  */

trait Processor {
  def DEFAULT_IN_PORT_NAMES(n: Int): Seq[String] = {
    (1 to n).map("_" + _);
  }

  def DEFAULT_OUT_PORT_NAMES(n: Int): Seq[String] = {
    (1 to n).map("_" + _);
  }
}

trait ProcessorN2N {
  def getInPortNames(): Seq[String];

  def getOutPortNames(): Seq[String];

  def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _];
}

trait Processor121 extends Processor {
  def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);

  def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);

  def perform(input: Any, ctx: RunnerContext): Any;
}

trait Processor12N extends Processor {
  def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);

  def getOutPortNames(): Seq[String];

  def perform(input: Any, ctx: RunnerContext): Map[String, _];
}

trait ProcessorN21 extends Processor {
  def getInPortNames(): Seq[String];

  def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);

  def perform(inputs: Map[String, _], ctx: RunnerContext): Any;
}

trait Processor021 extends Processor {
  def getOutPortName() = DEFAULT_OUT_PORT_NAMES(1)(0);

  def perform(ctx: RunnerContext): Any;
}

trait Processor02N extends Processor {
  def getOutPortNames(): Seq[String];

  def perform(ctx: RunnerContext): Map[String, _];
}

trait Processor020 extends Processor {
  def perform(ctx: RunnerContext): Unit;
}

trait Processor120 extends Processor {
  def getInPortName() = DEFAULT_IN_PORT_NAMES(1)(0);

  def perform(input: Any, ctx: RunnerContext): Unit;
}

trait ProcessorN20 extends Processor {
  def getInPortNames(): Seq[String];

  def perform(inputs: Map[String, _], ctx: RunnerContext): Unit;
}

class WrongProcessorException(processor: Processor) extends RuntimeException(
  String.format("wrong processor type of %s, expected: %s", processor.toString(), Seq(
    classOf[ProcessorN2N], classOf[Processor120], classOf[Processor021], classOf[Processor020], classOf[Processor121],
    classOf[Processor12N], classOf[ProcessorN21], classOf[Processor02N], classOf[ProcessorN20]))) {
}

object ProcessorN2N {
  def fromUnknown(processor: Processor): ProcessorN2N = {
    processor match {
      case x: Processor120 ⇒ from(x);
      case x: Processor121 ⇒ from(x);
      case x: Processor021 ⇒ from(x);
      case x: Processor020 ⇒ from(x);
      case x: Processor12N ⇒ from(x);
      case x: ProcessorN21 ⇒ from(x);
      case x: ProcessorN2N ⇒ x;
      case _ ⇒ throw new WrongProcessorException(processor);
    }
  }

  def from(processor: Processor120): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());

    override def getOutPortNames(): Seq[String] = Seq();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
      processor.perform(inputs(processor.getInPortName()), ctx);
      Map();
    }
  }

  def from(processor: Processor121): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());

    override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
      Map(processor.getOutPortName() -> processor.perform(inputs(processor.getInPortName()), ctx));
  }

  def from(processor: Processor021): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq();

    override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
      Map(processor.getOutPortName() -> processor.perform(ctx));
  }

  def from(processor: Processor12N): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq(processor.getInPortName());

    override def getOutPortNames(): Seq[String] = processor.getOutPortNames();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
      processor.perform(inputs(processor.getInPortName()), ctx);
  }

  def from(processor: ProcessorN21): ProcessorN2N = new ProcessorN2N {
    override def getOutPortNames(): Seq[String] = Seq(processor.getOutPortName());

    override def getInPortNames(): Seq[String] = processor.getInPortNames();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
      Map(processor.getOutPortName() -> processor.perform(inputs, ctx));
  }

  def from(processor: Processor020): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq();

    override def getOutPortNames(): Seq[String] = Seq();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
      processor.perform(ctx);
      Map();
    }
  }

  def from(processor: Processor02N): ProcessorN2N = new ProcessorN2N {
    override def getInPortNames(): Seq[String] = Seq();

    override def getOutPortNames(): Seq[String] = processor.getOutPortNames();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] =
      processor.perform(ctx);
  }

  def from(processor: ProcessorN20): ProcessorN2N = new ProcessorN2N {
    override def getOutPortNames(): Seq[String] = Seq();

    override def getInPortNames(): Seq[String] = processor.getInPortNames();

    override def performN2N(inputs: Map[String, _], ctx: RunnerContext): Map[String, _] = {
      processor.perform(inputs, ctx);
      Map();
    }
  }
}