package spark

import org.apache.spark._


val tableName = "student1"

val conf = HBaseConfiguration.create()

conf.set("hbase.zookeeper.quorum", "9.115.65.92");

conf.set(TableInputFormat.INPUT_TABLE, tableName)

val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
classOf[org.apache.hadoop.hbase.client.Result])

val count = hBaseRDD.count()
print("HBase RDD count:" + count)


val namelist = hBaseRDD.map{ case (k,v) => var cell = v.getColumnLatestCell(Bytes.toBytes("personal"),Bytes.toBytes("name"));  new String(CellUtil.cloneValue(cell))}

val data = Array("Name01 Class01", "Name02 Class02")
val distData = sc.parallelize(data)

val myTable = new HTable(conf, tableName)


var p = new Put(Bytes.toBytes("hello"))
p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("hello-value"))
myTable.put(p)

distData.foreach(a=>{
    val a2 = a.split(" ")
     var p = new Put(Bytes.toBytes(a2(0)))
     p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(a2(1)))
      myTable.put(p)
    })

