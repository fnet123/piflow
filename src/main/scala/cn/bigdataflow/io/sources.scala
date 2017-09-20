package cn.bigdataflow.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import cn.bigdataflow.RunnerContext
import scala.reflect.ManifestFactory.classType
import org.apache.spark.sql.Dataset

/**
 * @author bluejoe2008@gmail.com
 */

case class SeqAsSource[X: Encoder](t: X*) extends BatchSource[X] {
	override def toString = this.getClass.getSimpleName;
	def createDataset(ctx: RunnerContext): Dataset[X] = {
		ctx.forType[SparkSession].createDataset(t);
	}
}