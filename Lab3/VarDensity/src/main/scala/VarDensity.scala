/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object VarDensity 
{
	final val compressRDDs = true
		
	def main(args: Array[String]) 
	{
		val tasks = args(0)
		val dbsnpFile = args(1)
		val dictFile = args(2)
		
		println(s"Tasks = $tasks\ndbsnpFile = $dbsnpFile\ndictFile = $dictFile\n")
			
		val conf = new SparkConf().setAppName("Variant Density Calculator App")
		conf.setMaster("local[" + tasks + "]")
		conf.set("spark.cores.max", tasks)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")
		val sc = new SparkContext(conf)
		
		val t0 = System.currentTimeMillis
		
		// Add your main code here
		
		sc.stop()
		
		val et = (System.currentTimeMillis - t0) / 1000
		println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))
	} 
}
