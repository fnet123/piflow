package cn.piflow.processor.ds

import cn.piflow.JobContext
import cn.piflow.processor.{Processor121, Processor12N, ProcessorN21}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, RelationalGroupedDataset}

import scala.reflect.ClassTag

/**
	* @author bluejoe2008@gmail.com
	*/
case class DoMap[X: Encoder, Y: Encoder](fn: X ⇒ Y) extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): Dataset[Y] = {
		input.asInstanceOf[Dataset[X]].map(fn);
	}
}

case class AsDataSet[Y: Encoder]() extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): Dataset[Y] = {
		input.asInstanceOf[Dataset[_]].as[Y];
	}
}

case class AsDataFrame() extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): DataFrame = {
		input.asInstanceOf[Dataset[_]].toDF;
	}
}

case class DoTransform[X, Y](fn: X ⇒ Y) extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): Y = {
		fn(input.asInstanceOf[X]);
	}
}

case class DoFlatMap[X: Encoder, Y: Encoder](fn: X ⇒ TraversableOnce[Y])
	extends Processor121 {
	def perform121(input: Any): Dataset[Y] = {
		input.asInstanceOf[Dataset[X]].flatMap(fn);
	}
}

case class DoFilter[X: Encoder](fn: X ⇒ Boolean) extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): Dataset[X] = {
		input.asInstanceOf[Dataset[X]].filter(fn);
	}
}

case class DoGroupBy[X: Encoder](key: String) extends Processor121 {
	override def toString = this.getClass.getSimpleName;

	def perform121(input: Any): RelationalGroupedDataset = {
		input.asInstanceOf[Dataset[X]].groupBy(key);
	}
}

case class DoFork[X](conditions: (X ⇒ Boolean)*) extends Processor12N {
	/**
		* creates N copies
		*/
	def this(ncopy: Int) = {
		this((1 to ncopy).map(n ⇒ (m: X) ⇒ true): _*)
	}

	override def toString = this.getClass.getSimpleName;

	def perform12N(input: Any): Map[String, _] = {
		val map = collection.mutable.Map[String, Dataset[Any]]();
		val conditionsMap = getOutPortNames.zip(conditions);
		conditionsMap.foreach { x ⇒
			map(x._1) = input.asInstanceOf[Dataset[X]].filter(x._2).asInstanceOf[Dataset[Any]];
		}

		map.toMap
	}

	def getOutPortNames(): Seq[String] = DEFAULT_OUT_PORT_NAMES(conditions.size);
}

case class DoZip[X: Encoder, Y: Encoder]()(implicit ct: ClassTag[Y], en: Encoder[(X, Y)]) extends ProcessorN21 {
	def performN21(inputs: Map[String, _]): Dataset[(X, Y)] = {
		val ds1: Dataset[X] = inputs(getInPortNames().head).asInstanceOf[Dataset[X]];
		val ds2: Dataset[Y] = inputs(getInPortNames()(1)).asInstanceOf[Dataset[Y]];
		ds1.sparkSession.createDataset(ds1.rdd.zip(ds2.rdd));
	}

	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);
}

case class DoMerge[X: Encoder]() extends ProcessorN21 {
	def performN21(inputs: Map[String, _]): Dataset[X] = {
		inputs(getInPortNames().head).asInstanceOf[Dataset[X]]
			.union(inputs(getInPortNames()(1)).asInstanceOf[Dataset[X]]);
	}

	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);
}

