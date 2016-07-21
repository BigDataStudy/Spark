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

object SparkKafka {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def insertToHbaseByPartions(p: Iterator[(String, String, Int, Int)]): Iterator[Int] = {
    val list = p.toList;

    val hConf = new HBaseConfiguration()
    hConf.set("hbase.zookeeper.quorum", "9.115.65.92");
    val hTable = new HTable(hConf, "didi:order_statistics")
    //x=> rowkey, family, request, reply
    list.map {
      x =>

        val rowKey = x._1
        val family_slot = x._2

        val get = new Get(Bytes.toBytes(rowKey));
        val resultSet = hTable.get(get);

        var old_request = "0";
        var old_reply = "0";
        try {
          old_request = new String(CellUtil.cloneValue(resultSet.getColumnLatestCell(Bytes.toBytes(family_slot), Bytes.toBytes("request"))))
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
        try {
          old_reply = new String(CellUtil.cloneValue(resultSet.getColumnLatestCell(Bytes.toBytes(family_slot), Bytes.toBytes("reply"))))
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        println("===hbase current >" + old_request + "," + old_reply)

        val requst = Integer.valueOf(x._3).toInt + Integer.valueOf(old_request)
        val reply = Integer.valueOf(x._4).toInt + Integer.valueOf(old_reply)

        println("===new >" + requst + "," + reply)
        val thePut = new Put(Bytes.toBytes(rowKey))
        thePut.addColumn(Bytes.toBytes(family_slot), Bytes.toBytes("request"), Bytes.toBytes(requst.toString()))
        thePut.addColumn(Bytes.toBytes(family_slot), Bytes.toBytes("reply"), Bytes.toBytes(reply.toString()))
        hTable.put(thePut)
    }

    var res = List[Int]()
    res.iterator
  }

  //return rowkey, family, request, reply
  //input kafka value
  def processRDD(line: String): (String, String, Int, Int) = {
    val array = line.split("\t");
    var request = 0;
    var response = 0;
    if (array(1) == "NULL") { request = 1; response = 0 };
    else { request = 0; response = 1 };
    (array(3) + "-" + getDate(array(6)), getRank(array(6)), request, response);
  }

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

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("kafka-test");
    val sc = new SparkContext(conf)
    val accum = sc.accumulator(0, "My Accumulator")

    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("checkpoint")

    val topicMap = Map(("didi-order-topic", 3));
    val kafkaStream = KafkaUtils.createStream(ssc, "BTTETI01:2181,BTTETI02:2181,BTTETI03:2181", "group-didi-order", topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //val lines = kafkaStream.map(_._2)
    val lines = kafkaStream.window(Minutes(1), Minutes(1)).map(_._2)

    // val hbaseInput = lines.window(Minutes(1), Minutes(1)).map(processRDD(_));
    val hbaseInput = lines.map { x => accum += 1; processRDD(x); };

    println("====================================Spark receive message from kafka===============================================================" + accum.toString())

    val d = hbaseInput.mapPartitions(insertToHbaseByPartions(_), false)
    d.print();

    ssc.start()
    ssc.awaitTermination()
    println("====================================Spark  kafka exit===============================================================" + accum.toString())
  }
}

