/**
 
 $SPARK_HOME/bin/spark-submit \
 --class WordCount \
 --master local[8] \
 $SPARK_HOME/lib/demo-project_2.11-1.0.jar \
 hdfs://9.125.73.223:9000/user/hive/warehouse/d_tr_order_data/order_data_2016-01-01
 
 
hadoop jar \
/root/hadoop-2.7.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar \
wordcount \
"hdfs://9.125.73.223:9000/user/hive/warehouse/d_tr_order_data/order_data_2016-01-01" \
"out01"

**/
 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * 统计字符出现次数
 */
  
object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    

    line.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}