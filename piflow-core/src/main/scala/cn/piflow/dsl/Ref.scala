package cn.piflow.dsl

import cn.piflow.FlowNode
import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor

/**
  * @author bluejoe2008@gmail.com
  */
trait Ref[T] {
  type BoundType <: BoundNode[T];

  private var _bound: BoundType = null.asInstanceOf[BoundType];
  private var _node: FlowNode = null.asInstanceOf[FlowNode];

  def bind(bound: BoundType) = _bound = bound;

  def bind(node: FlowNode) = _node = node;

  def as[T]: T = get.asInstanceOf[T];

  def get: T = _bound.userData;

  def bound: BoundType = _bound;

  def node: FlowNode = _node;

  def processor: Processor = _node.processor;
}

case class SourceRef() extends Ref[BatchSource] {
  override type BoundType = SourceNode;
}

case class SinkRef() extends Ref[Sink] {
  override type BoundType = SinkNode;
}

case class ProcessorRef() extends Ref[Processor] {
  override type BoundType = ProcessorNode;
}