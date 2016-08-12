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

object SparkLogistic {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


  def getRank(date: String): String = {
    var time = date.split(" ")(1)
    var hour = time.split(":")(0)
    var min = time.split(":")(1)
    var rank = hour.toInt * 6 + min.toInt / 10 + 1
    rank.toString()
  }

  def getDate(date: String): String = {
    date.split(" ")(0)
  }
  
  def transition(date: String, time: String, driverId: String): ((String, String), (Int, Int)) = {
     if (driverId == "NULL") {
       ((date,time),(1,0))
     } else {
       ( (date,time),(1,1))
     }
  }
  
  
  def main(args: Array[String])= {
    val conf = new SparkConf().setAppName("kafka-test")
    val sc = new SparkContext(conf)
    
    val file = sc.textFile("file:///root/data/didi/season_1/training_data/order_data/*", 4);
    val one_orgial = file.map { line => line.split("\t")  }.filter { x => x(3).equals("1afd7afbc81ecc1b13886a569d869e8a") };
    val data_time_rr = one_orgial.map { x => transition(getDate(x(6)), getRank(x(6)), x(1)) };
    val data_time_request = data_time_rr.mapValues{value => value._1}.reduceByKey(_+_)
    val data_time_reply = data_time_rr.mapValues{value => value._2}.reduceByKey(_+_)
    val request_reply = data_time_request.join(data_time_reply)
    val time_request_gap = request_reply.map{case((data,time),(request,reply)) => ((data,time),(time,request,request-reply))}
    
    val output = time_request_gap.mapValues{case(a,b,c) =>  if(c>40){(a,b,c,1)}else{(a,b,c,0)} }
    
    output.values.saveAsTextFile("file:///root/data/time_request_gap");
  }



  
 
}

