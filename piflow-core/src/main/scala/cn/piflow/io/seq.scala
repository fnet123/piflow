package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
	* @author bluejoe2008@gmail.com
	*/

case class SeqAsSource[X: Encoder](t: X*) extends BatchSource {
	override def toString = this.getClass.getSimpleName;
	var _spark: SparkSession = null;

	override def init(ctx: JobContext): Unit = {
		_spark = ctx.sparkSession();
	}

	override def destroy() {}

	def loadBatch(): Dataset[X] = {
		_spark.createDataset(t);
	}
}