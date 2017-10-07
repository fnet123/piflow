package cn.piflow.dsl

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.{FlowGraph, ProcessorNode}

/**
  * Created by bluejoe on 2017/10/1.
  */
trait Chaining[X] {
  def >>(sink: SinkArrow): ChainWithSinkAsTail = {
    pipeNext(sink, true);
  }

  def >(sink: SinkArrow): ChainWithSinkAsTail = {
    pipeNext(sink, false);
  }

  def >(processor: ProcessorArrow): ChainWithProcessorAsTail = {
    pipeNext(processor, false);
  }

  def pipeNext(nwp: ProcessorArrow, append: Boolean): ChainWithProcessorAsTail = {
    new ChainWithProcessorAsTail(tail(), nwp);
  }

  def >(ref: ProcessorRef): ChainWithProcessorAsTail = {
    pipeNext(ref.bound, false);
  }

  def >(ref: SinkRef): ChainWithSinkAsTail = {
    pipeNext(ref.bound, false);
  }

  def pipeNext(nwp: SinkArrow, append: Boolean): ChainWithSinkAsTail = {
    new ChainWithSinkAsTail(tail(), nwp);
  }

  def >>(ref: SinkRef): ChainWithSinkAsTail = {
    pipeNext(ref.bound, true);
  }

  def tail(): Arrow[X];

  def pipeNext(nwp: SourceArrow, append: Boolean): ChainWithSourceAsTail = {
    new ChainWithSourceAsTail(tail(), nwp);
  }
}

abstract class ChainWithTail[Y](val current: Arrow[_], val successor: Arrow[Y])
  extends Chaining[Y] {
  //create links
  current.bound.addSuccessor(successor.bound, current.ports);
  successor.bound.addPredecessor(current.bound, current.ports);

  override def tail(): Arrow[Y] = successor;

  def bindFlowGraph(flowGraph: FlowGraph) {
    //calculates all involved bounds
    val involvedBounds = collection.mutable.Map[BoundNode[_], Object]();
    visitBound(current.bound, involvedBounds);

    //create processor nodes for each bound
    val allNodes = collection.mutable.Map[BoundNode[_], ProcessorNode]();
    involvedBounds.keys.foreach { bound: BoundNode[_] =>
      //FIXME: append or overwrite
      val node = flowGraph.createNode(bound.createProcessor());
      bound.notifyRefs(node);
      allNodes(bound) = node;
    }

    //create links
    involvedBounds.keys.foreach { bound: BoundNode[_] =>
      bound.successors.foreach { edge: Sibling =>
        flowGraph.link(allNodes(bound), allNodes(edge.target), edge.ports)
      }
    }

    flowGraph;
  }

  private def visitBound(bound: BoundNode[_], involvedBounds: collection.mutable.Map[BoundNode[_], Object]) {
    if (!involvedBounds.contains(bound)) {
      involvedBounds(bound) = new Object();

      bound.predecessors.foreach { edge: Sibling =>
        visitBound(edge.target, involvedBounds);
      }

      bound.successors.foreach { edge: Sibling =>
        visitBound(edge.target, involvedBounds);
      }
    }
  }
}

class ChainWithSourceAsTail(currentNode: Arrow[_], successorNode: SourceArrow)
  extends ChainWithTail[BatchSource](currentNode, successorNode) {
}

class ChainWithSinkAsTail(currentNode: Arrow[_], successorNode: SinkArrow)
  extends ChainWithTail[Sink](currentNode, successorNode) {
}

class ChainWithProcessorAsTail(currentNode: Arrow[_], successorNode: ProcessorArrow)
  extends ChainWithTail[Processor](currentNode, successorNode) {
}
