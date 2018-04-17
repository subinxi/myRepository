# myRepository
package com.demo
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.KeyValue;
class hbaseTest {
  val spark = SparkSession.builder().appName("HBaseTest").enableHiveSupport().getOrCreate()
  import spark.implicits._
  @transient
  val conf = HBaseConfiguration.create()  
  val tablename ="TM_PROCESS_BAOGONG"   
  //设置zooKeeper集群地址
  conf.set("hbase.zookeeper.quorum","******")  
  //设置zookeeper连接端口，默认2181  
  conf.set("hbase.zookeeper.property.clientPort", "2181") 
  conf.set(TableInputFormat.INPUT_TABLE, tablename)
  val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, 
            classOf[TableInputFormat],  
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],  
            classOf[org.apache.hadoop.hbase.client.Result]);
  hBaseRDD.cache()
  val arr=hBaseRDD.collect()
  var listTmp: List[String] = List[String]();
  for(i <-0 to arr.length-1){
    val str=arr(i)._2
    val str1=str.toString()
    val str2=str1.split(",")
    for(j <-0 to str2.length-1){
      val len1=str2(j).indexOf(":")
      val str3=str2(j).substring(len1+1, str2(j).length())
      val len2=str3.indexOf("/")
      val str4=str3.substring(0, len2)
      listTmp=str4::listTmp
    }
  }
  //获取所有列
  val list=listTmp.distinct  
  //打印rowkey和列名以及对应的值
  for(i <- 0 to arr.length-1){
    val result=arr(i)._2
    val key = Bytes.toString(result.getRow)//行键 
    println(key)
    val kvs=result.raw()
    var str=""
    for(kv <- kvs){
      str+=Bytes.toString(kv.getQualifier)+":"+Bytes.toString(kv.getValue)+"`"
    }
    println(str.substring(0, str.length()-1)) 
  }
  
  
  val df1=hBaseRDD.map({ case (_,result) =>
      val key = Bytes.toString(result.getRow) //获取行键 
       //通过列族和列名获取列  
      val ACT_QTY = Bytes.toString(result.getValue("aoke".getBytes,"ACT_QTY".getBytes))
      val ACT_WEIGHT = Bytes.toString(result.getValue("aoke".getBytes,"ACT_WEIGHT".getBytes))
      val BAOG_NG_QTY = Bytes.toString(result.getValue("aoke".getBytes,"BAOG_NG_QTY".getBytes))
      (key,name,gender,age)
  }).toDF("key","ACT_QTY","ACT_WEIGHT","BAOG_NG_QTY")
  
  val df2=hBaseRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow) 
      val kvs=result.raw()
      var str=""
    for(kv <- kvs){
      str+=Bytes.toString(kv.getQualifier)/*+":"+Bytes.toString(kv.getValue)*/+"`"
    }
    println(str.substring(0, str.length()-1))
  })
}
