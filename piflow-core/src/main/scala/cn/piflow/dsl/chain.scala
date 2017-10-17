package cn.piflow.dsl

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.Processor
import cn.piflow.{FlowGraph, FlowNode}
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/1.
	*/
trait Chaining[X] {
	def >>(sink: SinkArrow): ChainWithSinkAsTail = {
		pipeNext(sink, OutputMode.Append());
	}

	def >(sink: SinkArrow): ChainWithSinkAsTail = {
		pipeNext(sink, OutputMode.Complete());
	}

	def >(source: SourceArrow): ChainWithSourceAsTail = {
		pipeNext(source);
	}

	def >(processor: ProcessorArrow): ChainWithProcessorAsTail = {
		pipeNext(processor);
	}

	def >>(sink: SinkNode): ChainWithSinkAsTail = {
		pipeNext(new SinkArrow(sink), OutputMode.Append());
	}

	def >(sink: SinkNode): ChainWithSinkAsTail = {
		pipeNext(new SinkArrow(sink), OutputMode.Complete());
	}

	def >(source: SourceNode): ChainWithSourceAsTail = {
		pipeNext(new SourceArrow(source));
	}

	def >(processor: ProcessorNode): ChainWithProcessorAsTail = {
		pipeNext(new ProcessorArrow(processor));
	}

	def >(ref: ProcessorRef): ChainWithProcessorAsTail = {
		pipeNext(new ProcessorArrow(ref.bound));
	}

	def >(ref: SinkRef): ChainWithSinkAsTail = {
		pipeNext(new SinkArrow(ref.bound), OutputMode.Complete());
	}

	def >(ref: SourceRef): ChainWithSourceAsTail = {
		pipeNext(new SourceArrow(ref.bound));
	}

	def >>(ref: SinkRef): ChainWithSinkAsTail = {
		pipeNext(new SinkArrow(ref.bound), OutputMode.Append());
	}

	def tail(): Arrow[X];

	def pipeNext(nwp: SinkArrow, outputMode: OutputMode): ChainWithSinkAsTail = {
		nwp.node.setOutputMode(outputMode);
		new ChainWithSinkAsTail(tail(), nwp);
	}

	def pipeNext(nwp: SourceArrow): ChainWithSourceAsTail = {
		new ChainWithSourceAsTail(tail(), nwp);
	}

	def pipeNext(nwp: ProcessorArrow): ChainWithProcessorAsTail = {
		new ChainWithProcessorAsTail(tail(), nwp);
	}
}

abstract class ChainWithTail[Y](val current: Arrow[_], val successor: Arrow[Y])
	extends Chaining[Y] {
	//create links
	current.node.addSuccessor(successor.node, current.ports);
	successor.node.addPredecessor(current.node, current.ports);

	override def tail(): Arrow[Y] = successor;

	def bindFlowGraph(flowGraph: FlowGraph) {
		//TODO: sort nodes by dependencies
		//calculates all involved bounds
		val involvedBounds = collection.mutable.Map[BoundNode[_], Object]();
		visitBound(current.node, involvedBounds);

		//create processor nodes for each bound
		val allNodes = collection.mutable.Map[BoundNode[_], FlowNode]();
		involvedBounds.keys.foreach { bound: BoundNode[_] =>
			val node = flowGraph.createNode(bound.createProcessor());
			bound.notifyRefs(node);
			allNodes(bound) = node;
		}

		//create links
		involvedBounds.keys.foreach { bound: BoundNode[_] =>
			bound.successors.foreach { edge: Relationship =>
				flowGraph.link(allNodes(bound), allNodes(edge.target), edge.ports)
			}
		}
	}

	private def visitBound(bound: BoundNode[_], involvedBounds: collection.mutable.Map[BoundNode[_], Object]) {
		if (!involvedBounds.contains(bound)) {
			involvedBounds(bound) = new Object();

			bound.predecessors.foreach { edge: Relationship =>
				visitBound(edge.target, involvedBounds);
			}

			bound.successors.foreach { edge: Relationship =>
				visitBound(edge.target, involvedBounds);
			}
		}
	}
}

//TODO: > source?
class ChainWithSourceAsTail(currentNode: Arrow[_], successorNode: SourceArrow)
	extends ChainWithTail[BatchSource](currentNode, successorNode) {
}

class ChainWithSinkAsTail(currentNode: Arrow[_], successorNode: SinkArrow)
	extends ChainWithTail[Sink](currentNode, successorNode) {
}

class ChainWithProcessorAsTail(currentNode: Arrow[_], successorNode: ProcessorArrow)
	extends ChainWithTail[Processor](currentNode, successorNode) {
}