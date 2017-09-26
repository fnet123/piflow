package cn.bigdataflow.processor.transform

import scala.reflect.ClassTag
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import cn.bigdataflow.RunnerContext
import cn.bigdataflow.io.BatchSink
import cn.bigdataflow.io.BatchSource
import org.apache.spark.sql.Row
import scala.reflect.ManifestFactory.classType
import org.apache.spark.sql.Row
import cn.bigdataflow.processor.Processor121
import cn.bigdataflow.processor.Processor021
import cn.bigdataflow.processor.Processor12N
import cn.bigdataflow.processor.ProcessorN21
import cn.bigdataflow.processor.Processor120

/**
 * @author bluejoe2008@gmail.com
 */
case class DoMap[X: Encoder, Y: Encoder](fn: X ⇒ Y) extends Processor121[Dataset[X], Dataset[Y]] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[Y] = {
		input.map(fn);
	}
}

case class DoTransform[X, Y](fn: Dataset[X] ⇒ Dataset[Y]) extends Processor121[Dataset[X], Dataset[Y]] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[Y] = {
		fn(input);
	}
}

case class DoFlatMap[X: Encoder, Y: Encoder](fn: X ⇒ TraversableOnce[Y]) extends Processor121[Dataset[X], Dataset[Y]] {
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[Y] = {
		input.flatMap(fn);
	}
}

case class DoFilter[X: Encoder](fn: X ⇒ Boolean) extends Processor121[Dataset[X], Dataset[X]] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): Dataset[X] = {
		input.filter(fn);
	}
}

case class DoGroupBy[X: Encoder](key: String) extends Processor121[Dataset[X], RelationalGroupedDataset] {
	override def toString = this.getClass.getSimpleName;
	def perform(input: Dataset[X], ctx: RunnerContext): RelationalGroupedDataset = {
		input.groupBy(key);
	}
}

case class DoFork[X](conditions: X ⇒ Boolean*) extends Processor12N[Dataset[X]] {
	/**
	 * creates N copies
	 */
	def this(ncopy: Int) = { this((1 to ncopy).map(n ⇒ (m: X) ⇒ true): _*) }
	override def toString = this.getClass.getSimpleName;
	def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(conditions.size);
	def perform(input: Dataset[X], ctx: RunnerContext): Map[String, _] = {
		val map = collection.mutable.Map[String, Dataset[Any]]();
		val conditionsMap = getOutPortNames.zip(conditions);
		conditionsMap.foreach { x ⇒
			map(x._1) = input.filter(x._2).asInstanceOf[Dataset[Any]];
		}

		map.toMap
	}
}

object DoLoad {
	def apply[X: Encoder](format: String, args: Map[String, String] = Map()) = {
		new DoLoadDefinedSource(format, args);
	}

	def apply[X: Encoder](source: BatchSource[X]) = {
		new DoLoadSource(source);
	}
}

case class DoLoadSource[X](source: BatchSource[X]) extends Processor021[Dataset[X]] {
	override def perform(ctx: RunnerContext): Dataset[X] = {
		source.createDataset(ctx);
	}
}

case class DoLoadDefinedSource[X: Encoder](format: String, args: Map[String, String]) extends Processor021[Dataset[X]] {
	override def perform(ctx: RunnerContext): Dataset[X] = {
		ctx.forType[SparkSession].read.format(format).options(args).load().as[X];
	}
}

case class DoLoadStream[X: Encoder](format: String, args: Map[String, String])(implicit m: ClassTag[X]) extends Processor021[Dataset[X]] {
	override def perform(ctx: RunnerContext): Dataset[X] = {
		val df = ctx.forType[SparkSession].readStream.format(format).options(args).load();
		if (classOf[Row].isAssignableFrom(m.runtimeClass))
			df.asInstanceOf[Dataset[X]];
		else
			df.as[X];
	}
}

case class DoWrite[X](sinks: BatchSink[X]*) extends Processor120[Dataset[X]] {
	override def toString = {
		val as = ArrayUtils.toString(sinks.map(_.toString()).toArray);
		String.format("%s(%s)", this.getClass.getSimpleName, as.substring(1, as.length() - 1));
	}

	override def perform(input: Dataset[X], ctx: RunnerContext) {
		val writeHandlers = sinks.map { x ⇒
			x.consumeDataset(input, ctx);
		};
	}
}

case class DoWriteStream[X](queryName: String, format: String, outputMode: OutputMode, args: Map[String, String] = Map())
		extends Processor120[Dataset[X]] {
	override def perform(input: Dataset[X], ctx: RunnerContext) {
		val query = input.writeStream.options(args).format(format).outputMode(outputMode).queryName(queryName).start();
		ctx(queryName) = query;
		query.awaitTermination();
	}
}

case class DoZip[X: Encoder, Y: Encoder]()(implicit ct: ClassTag[Y], en: Encoder[(X, Y)]) extends ProcessorN21[Dataset[(X, Y)]] {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);

	def perform(inputs: Map[String, _], ctx: RunnerContext): Dataset[(X, Y)] = {
		val ds1: Dataset[X] = inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]];
		val ds2: Dataset[Y] = inputs(getInPortNames()(1)).asInstanceOf[Dataset[Y]];
		ds1.sparkSession.createDataset(ds1.rdd.zip(ds2.rdd));
	}
}

case class DoMerge[X: Encoder]() extends ProcessorN21[Dataset[X]] {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);
	def perform(inputs: Map[String, _], ctx: RunnerContext): Dataset[X] = {
		inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]]
			.union(inputs(getInPortNames()(1)).asInstanceOf[Dataset[X]]);
	}
}