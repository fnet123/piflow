package cn.piflow.processor.io

import scala.reflect.ClassTag
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import cn.piflow.RunnerContext
import cn.piflow.io.BatchSink
import cn.piflow.io.BatchSource
import scala.reflect.ManifestFactory.classType
import cn.piflow.processor.Processor021
import cn.piflow.processor.ProcessorN21
import cn.piflow.processor.Processor120
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author bluejoe2008@gmail.com
 */
object DoLoad {
	def apply(format: String, args: Map[String, String] = Map()) = {
		new DoLoadDefinedSource(format, args);
	}

	def apply(source: BatchSource) = {
		new DoLoadSource(source);
	}
}

case class DoLoadSource(source: BatchSource) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		source.createDataset(ctx);
	}
}

case class DoLoadDefinedSource(format: String, args: Map[String, String]) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		ctx.forType[SparkSession].read.format(format).options(args).load();
	}
}

case class DoLoadStream(format: String, args: Map[String, String]) extends Processor021 {
	override def perform(ctx: RunnerContext): Dataset[_] = {
		val df = ctx.forType[SparkSession].readStream.format(format).options(args).load();
		df;
	}
}

case class DoWrite(sinks: BatchSink*) extends Processor120 {
	override def toString = {
		val as = ArrayUtils.toString(sinks.map(_.toString()).toArray);
		String.format("%s(%s)", this.getClass.getSimpleName, as.substring(1, as.length() - 1));
	}

	override def perform(input: Any, ctx: RunnerContext) {
		val writeHandlers = sinks.map { x ⇒
			x.consumeDataset(input.asInstanceOf[Dataset[_]], ctx);
		};
	}
}

object DoWriteStream {
	val queryId = new AtomicInteger(0);
	def getNextQueryId() = "query-" + queryId.incrementAndGet();
}

case class DoWriteStream(format: String, outputMode: OutputMode, args: Map[String, String] = Map())
		extends Processor120 {
	override def perform(input: Any, ctx: RunnerContext) {
		val query = input.asInstanceOf[Dataset[_]].writeStream.options(args).format(format)
			.outputMode(outputMode).queryName(DoWriteStream.getNextQueryId).start();
		query.awaitTermination();
	}
}

case class DoZip[X: Encoder, Y: Encoder]()(implicit ct: ClassTag[Y], en: Encoder[(X, Y)]) extends ProcessorN21 {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);

	def perform(inputs: Map[String, _], ctx: RunnerContext): Dataset[(X, Y)] = {
		val ds1: Dataset[X] = inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]];
		val ds2: Dataset[Y] = inputs(getInPortNames()(1)).asInstanceOf[Dataset[Y]];
		ds1.sparkSession.createDataset(ds1.rdd.zip(ds2.rdd));
	}
}

