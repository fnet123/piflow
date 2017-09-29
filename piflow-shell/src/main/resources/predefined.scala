import cn.piflow._
import org.apache.spark.sql._

object PRELOAD_CODES {
	implicit val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();

	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");;

	implicit val runner = Runner.sparkRunner(spark);
	val jobs = new cn.piflow.shell.cmd.JobCmd(runner);
	val store = new cn.piflow.shell.cmd.StoreCmd(runner);
}