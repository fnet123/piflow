package cn.bigdataflow

import org.apache.spark.sql.Dataset

package object sql {
	type LabledDatasets = Map[String, Dataset[Any]];
}