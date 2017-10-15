package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
	* @author bluejoe2008@gmail.com
	*/

case class SeqAsSource[X: Encoder](t: X*) extends BatchSource {
	override def toString = this.getClass.getSimpleName;
	var _spark: SparkSession = null;

	override def init(ctx: RunnerContext): Unit = {
		_spark = ctx.forType[SparkSession];
	}

	override def destroy() {}

	def loadDataset(): Dataset[X] = {
		_spark.createDataset(t);
	}
}