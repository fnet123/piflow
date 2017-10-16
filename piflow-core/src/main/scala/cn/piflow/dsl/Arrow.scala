package cn.piflow.dsl

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.Processor
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/1.
	*/
abstract class Arrow[X](val node: BoundNode[X], val ports: (String, String) = "_1" -> "_1")
	extends Chaining[X] {
	override def tail(): Arrow[X] = this;
}

class SourceArrow(override val node: SourceNode, override val ports: (String, String) = "_1" -> "_1")
	extends Arrow[BatchSource](node, ports) {

}

class SinkArrow(override val node: SinkNode, override val ports: (String, String) = "_1" -> "_1")
	extends Arrow[Sink](node, ports) {

}

class ProcessorArrow(override val node: ProcessorNode, override val ports: (String, String) = "_1" -> "_1")
	extends Arrow[Processor](node, ports) {

}

class ArrowSeq[X](nodes: Seq[Arrow[X]])
	extends Chaining[X] {
	def tail(): Arrow[X] = nodes.head;

	override def pipeNext(nwp: SourceArrow): ChainWithSourceAsTail = {
		nodes.foreach(_.pipeNext(nwp));
		new ChainWithSourceAsTail(nodes.head, nwp);
	}

	override def pipeNext(nwp: SinkArrow, outputMode: OutputMode): ChainWithSinkAsTail = {
		nodes.foreach(_.pipeNext(nwp, outputMode));
		new ChainWithSinkAsTail(nodes.head, nwp);
	}

	override def pipeNext(nwp: ProcessorArrow): ChainWithProcessorAsTail = {
		nodes.foreach(_.pipeNext(nwp));
		new ChainWithProcessorAsTail(nodes.head, nwp);
	}
}