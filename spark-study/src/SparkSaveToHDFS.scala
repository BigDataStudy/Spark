import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.CellUtil
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.text.SimpleDateFormat;
import java.util.Date;

object SparkSaveToHDFS {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def insertToHDFS():Unit={
    
  }
  

  def getDate(date: String): String = {
    date.split(" ")(0)
  }

  def getRandom():Int={
     val rnd = new scala.util.Random
     rnd.nextInt(999999)
  }
  
  def getStrCurrentDate():String={
    val format=new SimpleDateFormat("yyyy-MM-dd");
	format.format(new Date());
  }
  	
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("kafka-writeto-hdfs");
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("checkpoint01")

    val topicMap = Map(("didi-order-topic", 3));
    val kafkaStream = KafkaUtils.createStream(ssc, "BTTETI01:2181,BTTETI02:2181,BTTETI03:2181", "group-didi-order", topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = kafkaStream.window(Minutes(5), Minutes(5)).map(_._2)
    
    lines.saveAsTextFiles("hdfs://9.115.65.91:9000/test/data/"+getStrCurrentDate()+"/"+getRandom())
   
    ssc.start()
    ssc.awaitTermination()
  }
}

