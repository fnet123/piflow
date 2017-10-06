package cn.piflow.dsl

/**
  * @author bluejoe2008@gmail.com
  */
trait Ref {
  private var _value: AnyRef = _;
  private var _node: PipedProcessorNode = _;

  def bindValue(value: AnyRef) = this._value = value;

  def bindNode(node: PipedProcessorNode) = this._node = node;

  def get: AnyRef = _value;

  def as[T]: T = _value.asInstanceOf[T];

  def node: PipedProcessorNode = _node;
}

case class SourceRef() extends Ref {
}

case class SinkRef() extends Ref {
}

case class ProcessorRef() extends Ref {
}