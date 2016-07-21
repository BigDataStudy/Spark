package spark

import org.apache.spark._

val data = Array("Name-1,Class01", "Name-2,Class02","Name-3,Class01", "Name-4,Class02","Name-5,Class01", "Name-6,Class02")
val distData = sc.parallelize(data, 2)


val insert = distData.mapPartitions{ p=>
    val list= p.toList
    var res = List[Int]()

    val hConf = new HBaseConfiguration() 
    hConf.set("hbase.zookeeper.quorum", "9.115.65.92");
    val hTable = new HTable(hConf, "student1") 
    list.map{
      x => 
      val line = x.split(",")  
      val thePut = new Put(Bytes.toBytes(line(0))) 
      thePut.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(line(1))) 
      hTable.put(thePut) 
    }
    

    res.iterator
}
insert.collect



distData.foreach { a =>
    val hConf = new HBaseConfiguration() 
    hConf.set("hbase.zookeeper.quorum", "9.115.65.92");
    val myTable = new HTable(hConf, "student1") 
    
    val a2 = a.split(" ")
    var p = new Put(Bytes.toBytes(a2(0)))
    p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(a2(1)))
    myTable.put(p)
  }


    

 distData.foreach(record => {
    val i = +1
    val hConf = new HBaseConfiguration() 
    hConf.set("hbase.zookeeper.quorum", "9.115.65.92");
    val hTable = new HTable(hConf, "student1") 
    val thePut = new Put(Bytes.toBytes(i)) 
    thePut.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(record)) 
    hTable.put(thePut)
  })
  
  

distData.mapPartitions{ p=>
    val hConf = new HBaseConfiguration() 
    hConf.set("hbase.zookeeper.quorum", "9.115.65.92");
    val myTable = new HTable(hConf, "student1") 
    
   
}
  
  
val a = sc.parallelize(1 to 10, 3)
def myfunc[T](iter: Iterator[T]) : Iterator[T]= {
   var res = List[Int]()
  while (iter.hasNext) {
    val cur = iter.next;
    res = res :: (cur)
  }
   res.iterator
}


a.mapPartitions(myfunc).collect
res0: Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))
  
