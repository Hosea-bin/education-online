package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object RegisterStreaming {
  //消费者组
  private val groupid="register_group_test"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)  //获取当前类名
    //控制消费者速度的参数，每个分区上每秒钟消费的条数
      .set("spark.streaming.kafka.maxRatePerPartition","100")  //10个
//      .set("spark.streaming.backpressure.enabled","true")
//      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .setMaster("local[*]")
    //第一个参数代表批次时间，即三秒一批数据
    val ssc = new StreamingContext(conf,Seconds(3))
    val sparkContext: SparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")//高可用，默认的系统和命名空间
    sparkContext.hadoopConfiguration.set("dfs.nameservices","nameservice1")
    //用数组存放topic,意思就是我这里监控多个topic
    val topics = Array("register_topic")
    //注意Map里面的泛型必须是String，和Object
    val kafkaMap: Map[String, Object] = Map[String, Object](
      //kafka监控地址
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //指定kafka反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest", //sparkStreaming 第一次启动，不丢数
      // 如果是true，消费者偏移量会在后台自动提交，但是kafka宕机容易丢数据
      // false,手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    kafkaMap
    //sparkStreaming 对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy
    val offsetMap = new mutable.HashMap[TopicPartition,Long]()
    val client: Connection = DataSourceUtil.getConnection
    try {
    sqlProxy.executeQuery(client,"select * from `offset_manager` where groupid= ? ",Array(groupid),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()){
          val model = new TopicPartition(rs.getString(2),rs.getInt(3))
          val offset: Long = rs.getLong(4)
          offsetMap.put(model,offset)
        }
        rs.close()//关闭游标
      }
    })
  }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费者数据的参数，判断本地是否有偏移量有则根据偏移量继续消费，无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if(offsetMap.isEmpty){
      KafkaUtils.createDirectStream(
        //第一个参数存放一个StreamingContext
        //第二个参数是消费数据的平衡策略，均匀消费
        //第三个参数就是具体要监控的对象，（里面包含topic,kafkaMap,偏移量）
        ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaMap))
      }else {
      KafkaUtils.createDirectStream(
        ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaMap,offsetMap))
    }


    //处理完 业务逻辑后，手动提交offset维护到本地mysql
    stream.foreachRDD(rdd =>{
      val proxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          proxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e :Exception=>e.printStackTrace()
      } finally {
        proxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }




}
