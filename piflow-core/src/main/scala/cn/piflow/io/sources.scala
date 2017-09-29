package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
 * @author bluejoe2008@gmail.com
 */

case class SeqAsSource[X: Encoder](t: X*) extends BatchSource {
	override def toString = this.getClass.getSimpleName;
	def createDataset(ctx: RunnerContext): Dataset[X] = {
		ctx.forType[SparkSession].createDataset(t);
	}
}