package sparkjob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkYarnJob {
	
	public static void main(String[] args) {
	
	
//	val spark = SparkSession.builder()
//		    .appName("Cloudera Sample Job")
//		    .master("yarn")
//		    .config("spark.hadoop.fs.defaultFS", "hdfs://132.252.198.42:8020")
//		    .config("spark.hadoop.yarn.resourcemanager.address", "132.252.198.42:8032")
//		    .config("spark.yarn.jars", "hdfs://132.252.198.42:8020/user/talentorigin/jars/*.jar")
//		    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
//		    .getOrCreate()
//		   
//		    println("================== Spark Sample Job ==================")
	
	
	
	SparkSession spark = SparkSession.builder()
			.appName("SparkYarnJobs")
			.master("yarn")
		    .config("spark.yarn.jars", "hdfs://132.252.198.42:8022/user/PC/jars/*.jar")
		    .enableHiveSupport()
			.getOrCreate();
	
	
	
	//read csv file from clouders hdfs
	Dataset<Row> data= spark.read()
			.format("csv")
			.option("header","true")
			.load("hdfs://132.252.198.42:8022/user/PC/data/stocks-data.csv");
	
	
	//write to hive table 
	data.write()
	    .partitionBy("cataloge")
	    .mode(SaveMode.Overwrite)
	    .saveAsTable("Stock.stocks_data");
	
	
	
	
			
	
	
	 System.out.print("Spark Sample Job "  );

}
}
