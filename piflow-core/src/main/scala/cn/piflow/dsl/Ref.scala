package cn.piflow.dsl

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.processor.io.DoWrite
import cn.piflow.{FlowException, FlowGraph, ProcessorNode}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.mutable.ArrayBuffer

/**
  * @author bluejoe2008@gmail.com
  */
trait RefTrait[T] {
  protected var value: T = null.asInstanceOf[T];

  def update(t: T) = value = t;

  def get: T = value;
}

case class Ref() extends RefTrait[Any] {
  def as[T]: T = value.asInstanceOf[T];
}

case class NodeRef() extends RefTrait[PipedProcessorNode] {
}