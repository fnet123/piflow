package cn.piflow.dsl

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor

/**
  * Created by bluejoe on 2017/10/1.
  */
abstract class Arrow[X](val bound: BoundNode[X], val ports: (String, String) = "_1" -> "_1")
  extends Chaining[X] {
  override def tail(): Arrow[X] = this;
}

class SourceArrow(override val bound: BoundSource, override val ports: (String, String) = "_1" -> "_1")
  extends Arrow[BatchSource](bound, ports) {

}

class SinkArrow(override val bound: BoundSink, override val ports: (String, String) = "_1" -> "_1")
  extends Arrow[Sink](bound, ports) {

}

class ProcessorArrow(override val bound: BoundProcessor, override val ports: (String, String) = "_1" -> "_1")
  extends Arrow[Processor](bound, ports) {

}

class ArrowSeq[X](nodes: Seq[Arrow[X]])
  extends Chaining[X] {
  def tail(): Arrow[X] = nodes(0);

  override def pipeNext(nwp: SourceArrow, append: Boolean): ChainWithSourceAsTail = {
    nodes.foreach(_.pipeNext(nwp, append));
    new ChainWithSourceAsTail(nodes(0), nwp);
  }

  override def pipeNext(nwp: SinkArrow, append: Boolean): ChainWithSinkAsTail = {
    nodes.foreach(_.pipeNext(nwp, append));
    new ChainWithSinkAsTail(nodes(0), nwp);
  }

  override def pipeNext(nwp: ProcessorArrow, append: Boolean): ChainWithProcessorAsTail = {
    nodes.foreach(_.pipeNext(nwp, append));
    new ChainWithProcessorAsTail(nodes(0), nwp);
  }
}