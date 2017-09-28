package cn.bigdataflow.processor.transform

import cn.bigdataflow.RunnerContext
import org.apache.spark.sql.Dataset
import cn.bigdataflow.processor.ProcessorN21
import org.apache.spark.sql.Encoder

/**
 * @author bluejoe2008@gmail.com
 */
case class DoMerge[X: Encoder]() extends ProcessorN21[Dataset[X]] {
	def getInPortNames(): Seq[String] = DEFAULT_IN_PORT_NAMES(2);
	def perform(inputs: Map[String, _], ctx: RunnerContext): Dataset[X] = {
		inputs(getInPortNames()(0)).asInstanceOf[Dataset[X]]
			.union(inputs(getInPortNames()(1)).asInstanceOf[Dataset[X]]);
	}
}